package auth_test

import (
	"strings"
	"testing"

	"github.com/querysheriff/backend/internal/auth"
)

const base62Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func TestHashToken(t *testing.T) {
	t.Parallel()

	const (
		input = "qsc_x"
		want  = "6a2fc0ccb94abd303a70ba0e54d9e9422efb337a59208383e8cbac1cafa44f2e"
	)

	if got := auth.HashToken(input); got != want {
		t.Fatalf("HashToken(%q) = %q, want %q", input, got, want)
	}

	if auth.HashToken("qsc_x") == auth.HashToken("qsc_y") {
		t.Fatal("HashToken collided on distinct inputs")
	}
}

func TestGenerateToken(t *testing.T) {
	t.Parallel()

	const attempts = 100

	seen := make(map[string]struct{}, attempts)

	for range attempts {
		token, err := auth.GenerateToken(auth.CollectorTokenPrefix)
		if err != nil {
			t.Fatalf("GenerateToken: %v", err)
		}

		body, ok := strings.CutPrefix(token, auth.CollectorTokenPrefix)
		if !ok {
			t.Fatalf("token %q missing prefix %q", token, auth.CollectorTokenPrefix)
		}

		if body == "" {
			t.Fatalf("token %q has empty body", token)
		}

		for _, r := range body {
			if !strings.ContainsRune(base62Alphabet, r) {
				t.Fatalf("token body %q has non-base62 rune %q", body, r)
			}
		}

		if _, dup := seen[token]; dup {
			t.Fatalf("GenerateToken produced a duplicate: %q", token)
		}

		seen[token] = struct{}{}
	}
}

func TestHashPasswordRoundTrip(t *testing.T) {
	t.Parallel()

	const password = "s3cret-passphrase"

	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	if hash == password {
		t.Error("HashPassword returned the password unchanged")
	}

	if !auth.CheckPassword(hash, password) {
		t.Error("CheckPassword rejected the correct password")
	}

	if auth.CheckPassword(hash, "wrong-password") {
		t.Error("CheckPassword accepted a wrong password")
	}

	if auth.CheckPassword("not-a-bcrypt-hash", password) {
		t.Error("CheckPassword accepted a malformed hash")
	}
}
