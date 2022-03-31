package passwd

import (
	"crypto/sha1"
	"crypto/sha256"

	"github.com/pingcap/tidb/parser/mysql"
)

// calculatePassword calculate password hash
func CalculatePassword(scramble, password []byte, authPlugin string) []byte {
	var authData []byte
	switch authPlugin {
	case mysql.AuthNativePassword:
		authData = CalcNativePassword(scramble, password)
	case mysql.AuthCachingSha2Password:
		authData = CalcCachingSha2Password(scramble, password)
	}
	return authData
}

func CalcNativePassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// CalcCachingSha2Password: Hash password using MySQL 8+ method (SHA256)
func CalcCachingSha2Password(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range stage1 {
		stage1[i] ^= message2[i]
	}

	return stage1
}
