// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

var _ io.WriteCloser = (*rotateWriter)(nil)

// rotateWriter is a writer that writes to a file and rotates the file when the
// file size reaches the configured limit. It also ensures that the file name
// is unique by appending a timestamp to the file name.
//
// Problems of lumberjack:
// - It doesn't support object storage.
// - The command size can't be bigger than the file size.
// - If it's written too frequently, the previous file may be overwritten.
type rotateWriter struct {
	cfg      WriterCfg
	storage  storage.ExternalStorage
	lg       *zap.Logger
	writeLen int
	buf      *bytes.Buffer
	// lastFileTS is the last timestamp string (formatted with logTimeLayout)
	// that was used in a file name. It's used to avoid creating multiple files
	// with the same timestamp-based name, which could overwrite the previous file
	// in object storage.
	lastFileTS string
}

func newRotateWriter(lg *zap.Logger, externalStorage storage.ExternalStorage, cfg WriterCfg) (*rotateWriter, error) {
	if cfg.FileSize == 0 {
		cfg.FileSize = fileSize
	}
	return &rotateWriter{
		cfg:     cfg,
		lg:      lg,
		storage: externalStorage,
		buf:     bytes.NewBuffer(make([]byte, 0, cfg.FileSize)),
	}, nil
}

func (w *rotateWriter) Write(data []byte) (n int, err error) {
	if len(data) == 0 {
		return 0, nil
	}
	if w.buf == nil {
		w.buf = bytes.NewBuffer(make([]byte, 0, w.cfg.FileSize))
	}
	if n, err = w.buf.Write(data); err != nil {
		return n, errors.WithStack(err)
	}
	w.writeLen += n
	if w.writeLen >= w.cfg.FileSize {
		// Normal rotation triggered by reaching FileSize. If the timestamp-based
		// file name would conflict with the last one, we prefer to keep writing
		// to the in-memory buffer and wait for a later opportunity (with a new
		// timestamp) to flush.
		err = w.flush(false)
	}
	return n, err
}

// flush writes current in-memory buffer to a new file. The file name contains
// the timestamp when the file is *closed*, which is consistent with TiDB audit
// log plugin files where the timestamp represents the end time of the file.
//
// If forceWait is false, and the newly generated timestamp string would be the
// same as the previous one, we skip flushing to avoid overwriting the last
// file and keep accumulating data in the buffer.
// If forceWait is true (used on Close), and the newly generated timestamp
// string would be the same as the previous one, we synthetically advance the
// timestamp by 1ms to guarantee both a flush and a non-conflicting file name
// without waiting.
func (w *rotateWriter) flush(force bool) error {
	if w.buf == nil || w.writeLen == 0 {
		return nil
	}
	var ext string
	if w.cfg.Compress {
		ext = fileCompressFormat
	}
	// Use timestamp in file name so that files can be efficiently located by time
	// in object storage (e.g. S3). The time format is the same as audit log
	// plugin files, but with a different prefix. The timestamp is generated
	// when the buffer is flushed to disk, which approximates the file end time.
	//
	// We compare formatted timestamp strings to avoid generating multiple files
	// with the same name:
	ts := time.Now().In(time.Local)
	tsStr := ts.Format(logTimeLayout)
	if w.lastFileTS != "" && tsStr == w.lastFileTS {
		if !force {
			// Continue to accumulate data in the current buffer until the timestamp
			// changes so we can safely generate a new, non-conflicting file.
			return nil
		}
		// Close() should not block waiting for the wall clock to advance. Instead,
		// if the formatted timestamp would collide with the previous one, bump the
		// timestamp by 1ms to ensure a strictly increasing suffix in the file
		// name.
		tsStr = ts.Add(time.Millisecond).Format(logTimeLayout)
	}
	w.lastFileTS = tsStr
	fileName := fmt.Sprintf("%s%s%s%s", fileNamePrefix, tsStr, fileNameSuffix, ext)
	// rotateWriter -> encryptWriter -> compressWriter -> file
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	fileWriter, err := w.storage.Create(ctx, fileName, &storage.WriterOption{})
	cancel()
	if err != nil {
		return errors.WithStack(err)
	}
	var writer io.WriteCloser = NewStorageWriter(fileWriter)
	if w.cfg.Compress {
		writer = newCompressWriter(w.lg, writer)
	}
	if writer, err = newWriterWithEncryptOpts(writer, w.cfg.EncryptionMethod, w.cfg.EncryptionKey); err != nil {
		return err
	}
	if _, err := writer.Write(w.buf.Bytes()); err != nil {
		_ = writer.Close()
		return errors.WithStack(err)
	}
	w.writeLen = 0
	w.buf.Reset()
	if err := writer.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *rotateWriter) Close() error {
	return w.flush(true)
}

type Reader interface {
	io.ReadCloser
	CurFile() string
}

var _ Reader = (*rotateReader)(nil)

type fileMeta struct {
	fileName string
	fileSize int64
}

type fileReader struct {
	fileName string
	reader   storage.ExternalFileReader
}

type rotateReader struct {
	cfg          ReaderCfg
	absolutePath string
	reader       io.Reader
	externalFile fileReader
	storage      storage.ExternalStorage
	lg           *zap.Logger
	fileCh       chan fileReader
	wg           waitgroup.WaitGroup
	cancel       context.CancelFunc
	eof          bool

	// fileMetaCache and fileMetaCacheIdx are used to access and cache file metadata.
	// The fileMeta in the cache should be sorted by file index in ascending order.
	fileMetaCache    []fileMeta
	fileMetaCacheIdx int
}

func newRotateReader(lg *zap.Logger, store storage.ExternalStorage, cfg ReaderCfg) (*rotateReader, error) {
	r := &rotateReader{
		cfg:     cfg,
		lg:      lg,
		storage: store,
		fileCh:  make(chan fileReader, 1),
	}
	childCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Run(func() {
		if err := r.openFileLoop(childCtx); err != nil && !errors.Is(err, io.EOF) {
			r.lg.Error("open file loop failed", zap.Error(err))
		}
	}, lg)
	return r, nil
}

func (r *rotateReader) Read(data []byte) (int, error) {
	if r.eof {
		r.lg.Info("rotateReader reached EOF", zap.String("file", r.absolutePath))
		return 0, io.EOF
	}
	if r.reader == nil || reflect.ValueOf(r.reader).IsNil() {
		if err := r.nextReader(); err != nil {
			return 0, err
		}
	}

	for {
		m, err := r.reader.Read(data[:])
		if err == nil {
			return m, nil
		}
		if !errors.Is(err, io.EOF) {
			return m, errors.WithStack(err)
		}
		_ = r.closeFile()
		if err := r.nextReader(); err != nil {
			r.eof = true
			return m, err
		}
		if m > 0 {
			return m, nil
		}
	}
}

func (r *rotateReader) CurFile() string {
	return r.absolutePath
}

func (r *rotateReader) Close() error {
	r.cancel()
	r.wg.Wait()
	for fr := range r.fileCh {
		if err := fr.reader.Close(); err != nil {
			r.lg.Warn("failed to close file", zap.Error(err))
		}
	}
	return r.closeFile()
}

func (r *rotateReader) closeFile() error {
	if r.externalFile.reader != nil && !reflect.ValueOf(r.externalFile.reader).IsNil() {
		if err := r.externalFile.reader.Close(); err != nil {
			r.lg.Warn("failed to close file", zap.String("filename", r.externalFile.fileName), zap.Error(err))
			return err
		}
		r.externalFile.reader = nil
		r.externalFile.fileName = ""
	}
	return nil
}

func (r *rotateReader) openFileLoop(ctx context.Context) error {
	var curFileTime time.Time
	var curFileName string
	var err error
	for ctx.Err() == nil {
		var minFileTime time.Time
		var minFileName string
		fileNamePrefix := getFileNamePrefix(r.cfg.Format)
		childCtx, cancel := context.WithTimeout(ctx, opTimeout)
		startTime := time.Now()
		err = r.walkFile(childCtx, curFileName,
			func(name string, size int64) (bool, error) {
				if !strings.HasPrefix(name, fileNamePrefix) {
					return false, nil
				}
				if !filterFileByTime(name, fileNamePrefix, r.cfg.FileNameFilterTime) {
					return false, nil
				}
				fileTime := parseFileTime(name, fileNamePrefix)
				if fileTime.IsZero() {
					r.lg.Warn("traffic file name is invalid", zap.String("filename", name), zap.String("format", r.cfg.Format))
					return false, nil
				}
				if !fileTime.After(curFileTime) {
					return false, nil
				}
				if minFileName == "" || fileTime.Before(minFileTime) {
					minFileTime = fileTime
					minFileName = name
					return true, nil
				}
				return false, nil
			})
		cancel()
		if err != nil {
			break
		}
		if minFileName == "" {
			if r.cfg.WaitOnEOF {
				time.Sleep(10 * time.Millisecond)
				continue
			} else {
				err = io.EOF
				break
			}
		}
		// storage.Open(ctx) stores the context internally for subsequent reads, so don't set a short timeout.
		var fr storage.ExternalFileReader
		fr, err = r.storage.Open(ctx, minFileName, &storage.ReaderOption{})
		if err != nil {
			err = errors.WithStack(err)
			break
		}
		curFileTime = minFileTime
		curFileName = minFileName
		r.lg.Info("opening next file", zap.String("file", path.Join(r.storage.URI(), minFileName)),
			zap.Duration("open_time", time.Since(startTime)),
			zap.Int("files_in_cache", len(r.fileMetaCache)-r.fileMetaCacheIdx))
		select {
		case r.fileCh <- fileReader{fileName: minFileName, reader: fr}:
		case <-ctx.Done():
		}
	}
	close(r.fileCh)
	return err
}

func (r *rotateReader) nextReader() error {
	fileReader, ok := <-r.fileCh
	if !ok {
		return io.EOF
	}
	r.reader = fileReader.reader
	r.externalFile = fileReader
	r.absolutePath = path.Join(r.storage.URI(), fileReader.fileName)
	r.lg.Info("reading file", zap.String("file", r.absolutePath))
	// rotateReader -> encryptReader -> compressReader -> file
	var err error
	if strings.HasSuffix(fileReader.fileName, fileCompressFormat) {
		if r.reader, err = newCompressReader(r.reader); err != nil {
			return err
		}
	}
	r.reader, err = newReaderWithEncryptOpts(r.reader, r.cfg.EncryptionMethod, r.cfg.EncryptionKey)
	return err
}

// walkFile walks through the files in the storage and applies the given function to each file.
// The return value of the function indicates whether this file is valid or not.
// For S3 storage and audit log format, it'll stop walking once the fn returns true.
func (r *rotateReader) walkFile(ctx context.Context, curfileName string, fn func(string, int64) (bool, error)) error {
	if s3, ok := r.storage.(*storage.S3Storage); ok {
		return r.walkS3(ctx, curfileName, s3.GetS3APIHandle(), s3.GetOptions(), fn)
	}
	return r.storage.WalkDir(ctx, &storage.WalkOption{}, func(name string, size int64) error {
		_, err := fn(name, size)
		return err
	})
}

// walkS3 is a special implementation to list files from S3.
// The reason is that the file name contains timestamp info, and we may
// want to start from a specific time point. The normal WalkDir implementation
// just lists all files in the directory, which is not efficient when there are
// many files.
// Most of the code is copied from storage/s3.go's WalkDir implementation.
func (r *rotateReader) walkS3(ctx context.Context, curFileName string, s3api s3iface.S3API, options *backuppb.S3, fn func(string, int64) (bool, error)) error {
	for ; r.fileMetaCacheIdx < len(r.fileMetaCache); r.fileMetaCacheIdx++ {
		meta := r.fileMetaCache[r.fileMetaCacheIdx]
		valid, err := fn(meta.fileName, meta.fileSize)
		if err != nil {
			return err
		}
		if valid {
			r.fileMetaCacheIdx++
			return nil
		}
	}

	// The cache is used up, and we didn't find the valid file yet.
	pathPrefix := options.Prefix
	if len(pathPrefix) > 0 && !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix += "/"
	}

	prefix := pathPrefix + getFileNamePrefix(r.cfg.Format)

	var marker string
	if curFileName != "" {
		marker = pathPrefix + curFileName
	} else if !r.cfg.FileNameFilterTime.IsZero() {
		t := r.cfg.FileNameFilterTime.In(time.Local)
		marker = fmt.Sprintf("%s%s", prefix, t.Format(logTimeLayout))
	}

	req := &s3.ListObjectsInput{
		Bucket:  aws.String(options.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
		Marker:  aws.String(marker),
	}
	// The first result of `ListObjects` may include the `marker` file itself, so
	// we still need to walk and filter it. It's possible to further optimize to
	// skip the first file and take the second one if the file name is exactly the
	// same with the `marker`.
	res, err := s3api.ListObjectsWithContext(ctx, req)
	if err != nil {
		return err
	}

	startAppendingFileToCache := false
	for _, file := range res.Contents {
		// when walk on specify directory, the result include storage.Prefix,
		// which can not be reuse in other API(Open/Read) directly.
		// so we use TrimPrefix to filter Prefix for next Open/Read.
		path := strings.TrimPrefix(*file.Key, options.Prefix)
		// trim the prefix '/' to ensure that the path returned is consistent with the local storage
		path = strings.TrimPrefix(path, "/")
		itemSize := *file.Size

		// filter out s3's empty directory items
		if itemSize <= 0 && strings.HasSuffix(path, "/") {
			continue
		}
		if startAppendingFileToCache {
			r.fileMetaCache = append(r.fileMetaCache, fileMeta{
				fileName: path,
				fileSize: itemSize,
			})
			continue
		}

		valid, err := fn(path, itemSize)
		if err != nil {
			return err
		}
		if valid {
			startAppendingFileToCache = true
			if r.fileMetaCache == nil {
				r.fileMetaCache = make([]fileMeta, 0, 1000)
			} else {
				r.fileMetaCache = r.fileMetaCache[:0]
			}
			r.fileMetaCacheIdx = 0
		}
	}

	return nil
}

func getFileNamePrefix(format string) string {
	switch format {
	case cmd.FormatAuditLogPlugin:
		return auditFileNamePrefix
	}
	return fileNamePrefix
}

// Parse the file name to get the file timestamp.
// filename pattern: tidb-audit-2025-09-10T17-01-56.073.log
func parseFileTime(name, fileNamePrefix string) time.Time {
	if !strings.HasPrefix(name, fileNamePrefix) {
		return time.Time{}
	}
	startIdx := len(fileNamePrefix)
	if len(name) <= startIdx+len(fileNameSuffix) {
		return time.Time{}
	}
	endIdx := len(name)
	if strings.HasSuffix(name, fileCompressFormat) {
		endIdx -= len(fileCompressFormat)
	}
	if !strings.HasSuffix(name[:endIdx], fileNameSuffix) {
		return time.Time{}
	}
	endIdx -= len(fileNameSuffix)
	// The `TimeZone` part is not included in the audit log file name, so we use the `time.Local` here.
	// It's always possible to workaround it by adjusting the commandStartTime, so just using `time.Local`
	// here is acceptable. Using the timezone from `commandStartTime` is another option, but it's a bit tricky.
	ts, err := time.ParseInLocation(logTimeLayout, name[startIdx:endIdx], time.Local)
	if err != nil {
		return time.Time{}
	}
	return ts
}

func filterFileByTime(name, fileNamePrefix string, fileNameFilterTime time.Time) bool {
	fileTime := parseFileTime(name, fileNamePrefix)
	if fileTime.IsZero() {
		return false
	}
	// Be careful that the log file name doesn't contain timezone info.
	// We assume the log file time is the Local time. But anyway we could workaround it by
	// adjusting the commandStartTime.
	return fileTime.After(fileNameFilterTime)
}
