// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	replaycmd "github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type corpus struct {
	Cases []replayCase `json:"cases"`
}

type replayCase struct {
	ID            string         `json:"id"`
	Format        string         `json:"format"`
	PreparedClose string         `json:"prepared_close"`
	FilterRetries bool           `json:"filter_retries"`
	UserAllowlist []string       `json:"user_allowlist"`
	SourceOrdinal int            `json:"source_ordinal"`
	Records       []nativeRecord `json:"records"`
	Lines         []string       `json:"lines"`
}

type nativeRecord struct {
	Headers    [][2]string `json:"headers"`
	PayloadHex string      `json:"payload_hex"`
}

type observation struct {
	CaseID               string `json:"case_id"`
	Index                uint64 `json:"index"`
	PayloadHex           string `json:"payload_hex"`
	StartUnixNanos       int64  `json:"start_unix_nanos"`
	EndUnixNanos         int64  `json:"end_unix_nanos"`
	ConnectionID         uint64 `json:"connection_id"`
	UpstreamConnectionID uint64 `json:"upstream_connection_id"`
	Command              byte   `json:"command"`
	CurrentDatabase      string `json:"current_database"`
	CapturedStatementID  uint32 `json:"captured_statement_id"`
	PreparedStatement    string `json:"prepared_statement"`
	StatementType        string `json:"statement_type"`
	Succeeded            bool   `json:"succeeded"`
}

func main() {
	corpusPath := flag.String("corpus", "", "shared replay corpus")
	baselinePath := flag.String("baseline", "", "baseline observation JSON")
	candidatePath := flag.String("candidate", "", "candidate observation JSON")
	flag.Parse()
	if *baselinePath != "" || *candidatePath != "" {
		if *baselinePath == "" || *candidatePath == "" {
			fatal(errors.New("both -baseline and -candidate are required"))
		}
		if err := compare(*baselinePath, *candidatePath); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_, _ = fmt.Fprintln(os.Stdout, `{"equal":true}`)
		return
	}
	if *corpusPath == "" {
		fatal(errors.New("-corpus is required"))
	}
	if err := observeCorpus(*corpusPath, os.Stdout); err != nil {
		fatal(err)
	}
}

func observeCorpus(path string, output io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var fixture corpus
	if err := json.NewDecoder(file).Decode(&fixture); err != nil {
		return err
	}
	observations := make([]observation, 0)
	for _, testCase := range fixture.Cases {
		input, err := buildInput(testCase)
		if err != nil {
			return fmt.Errorf("build %s: %w", testCase.ID, err)
		}
		decoder := replaycmd.NewCmdDecoder(
			replaycmd.TrafficFormat(testCase.Format),
			replaycmd.NewDeDup(),
			zap.NewNop(),
		)
		if audit, ok := decoder.(replaycmd.AuditLogDecoder); ok {
			audit.SetPSCloseStrategy(replaycmd.PSCloseStrategy(testCase.PreparedClose))
			allocator, err := replaycmd.NewConnIDAllocator(testCase.SourceOrdinal)
			if err != nil {
				return err
			}
			audit.SetIDAllocator(allocator)
			audit.SetUserAllowlist(testCase.UserAllowlist)
			if testCase.FilterRetries {
				audit.EnableFilterCommandWithRetry()
			}
		}
		reader := &memoryLineReader{name: testCase.ID, input: input}
		for index := uint64(0); ; index++ {
			command, err := decoder.Decode(reader)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("decode %s: %w", testCase.ID, err)
			}
			upstreamID := command.UpstreamConnID
			if upstreamID == 0 {
				upstreamID = command.ConnID
			}
			observations = append(observations, observation{
				CaseID:               testCase.ID,
				Index:                index,
				PayloadHex:           hex.EncodeToString(command.Payload),
				StartUnixNanos:       command.StartTs.UnixNano(),
				EndUnixNanos:         unixNanosOrZero(command.EndTs),
				ConnectionID:         command.ConnID,
				UpstreamConnectionID: upstreamID,
				Command:              command.Type.Byte(),
				CurrentDatabase:      command.CurDB,
				CapturedStatementID:  command.CapturedPsID,
				PreparedStatement:    command.PreparedStmt,
				StatementType:        command.StmtType,
				Succeeded:            command.Success,
			})
		}
	}
	return json.NewEncoder(output).Encode(observations)
}

func buildInput(testCase replayCase) ([]byte, error) {
	var input bytes.Buffer
	if testCase.Format == string(replaycmd.FormatNative) {
		for _, record := range testCase.Records {
			for _, header := range record.Headers {
				if _, err := fmt.Fprintf(&input, "# %s: %s\n", header[0], header[1]); err != nil {
					return nil, err
				}
			}
			payload, err := hex.DecodeString(record.PayloadHex)
			if err != nil {
				return nil, err
			}
			if _, err := fmt.Fprintf(&input, "# Payload_len: %d\n", len(payload)); err != nil {
				return nil, err
			}
			input.Write(payload)
			input.WriteByte('\n')
		}
	} else {
		for _, line := range testCase.Lines {
			input.WriteString(line)
			input.WriteByte('\n')
		}
	}
	return input.Bytes(), nil
}

func unixNanosOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

type memoryLineReader struct {
	name   string
	input  []byte
	offset int
	line   int
}

func (r *memoryLineReader) String() string { return r.name }

func (r *memoryLineReader) ReadLine() ([]byte, string, int, error) {
	if r.offset == len(r.input) {
		return nil, r.name, r.line, io.EOF
	}
	relativeEnd := bytes.IndexByte(r.input[r.offset:], '\n')
	if relativeEnd < 0 {
		return nil, r.name, r.line, io.ErrUnexpectedEOF
	}
	start := r.offset
	r.offset += relativeEnd + 1
	r.line++
	return r.input[start : r.offset-1], r.name, r.line, nil
}

func (r *memoryLineReader) Read(output []byte) (string, int, error) {
	if len(r.input)-r.offset < len(output) {
		return r.name, r.line, io.ErrUnexpectedEOF
	}
	copy(output, r.input[r.offset:r.offset+len(output)])
	r.offset += len(output)
	return r.name, r.line, nil
}

func (r *memoryLineReader) Close() {}

func compare(baselinePath, candidatePath string) error {
	read := func(path string) (any, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		return value, nil
	}
	baseline, err := read(baselinePath)
	if err != nil {
		return err
	}
	candidate, err := read(candidatePath)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(baseline, candidate) {
		return fmt.Errorf("decoder observations differ: baseline=%s candidate=%s", baselinePath, candidatePath)
	}
	return nil
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

var _ replaycmd.LineReader = (*memoryLineReader)(nil)
