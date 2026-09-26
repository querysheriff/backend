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
	base62               = 62
)

// GenerateToken creates a cryptographically random Base62 token with a prefix.
// Example: GenerateToken("qss_") -> "qss_8Fa2...".
func GenerateToken(prefix string) (string, error) {
	buf := make([]byte, tokenRandomBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	return prefix + new(big.Int).SetBytes(buf).Text(base62), nil
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

// DecoyHash is checked, result ignored, when no user has the email, so login timing can't reveal which emails exist.
const DecoyHash = "$2a$10$Ixy6iivTEt1hI3TJ5BcgQea7AXUHH9Ia5ytDe6Ejv80KluA0MOFam"

// CheckPassword reports whether a password matches a bcrypt hash.
// Example: CheckPassword(hash, "secret") -> true.
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}
