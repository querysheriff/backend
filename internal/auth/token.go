package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"

	"golang.org/x/crypto/bcrypt"
)

const (
	CollectorTokenPrefix = "qsc_"
	SessionTokenPrefix   = "qss_"
	tokenRandomBytes     = 20
	base62Base           = 62
	base62Alphabet       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// GenerateToken creates a cryptographically random Base62 token with a prefix.
// Example: GenerateToken("qss_") -> "qss_8Fa2...".
func GenerateToken(prefix string) (string, error) {
	buf := make([]byte, tokenRandomBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return prefix + base62Encode(buf), nil
}

// HashToken returns a SHA-256 hex hash of a token.
// Example: "qss_abc" -> "9f86...".
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))

	return hex.EncodeToString(sum[:])
}

// HashPassword hashes a password with bcrypt.
// Example: "secret" -> "$2a$10$...".
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	return string(hash), nil
}

// CheckPassword reports whether a password matches a bcrypt hash.
// Example: CheckPassword(hash, "secret") -> true.
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

func base62Encode(buf []byte) string {
	n := new(big.Int).SetBytes(buf)
	if n.Sign() == 0 {
		return base62Alphabet[:1]
	}

	base := big.NewInt(base62Base)
	zero := new(big.Int)
	mod := new(big.Int)

	var out []byte
	for n.Cmp(zero) > 0 {
		n.DivMod(n, base, mod)
		out = append(out, base62Alphabet[mod.Int64()])
	}

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return string(out)
}
