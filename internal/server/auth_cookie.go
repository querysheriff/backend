package server

import (
	"net/http"
	"time"
)

const (
	sessionCookieName = "querysheriff_session"
	sessionTTL        = 30 * 24 * time.Hour
)

// sessionCookie sets the session token; maxAge -1 clears it.
func sessionCookie(token string, maxAge int, secure bool) *http.Cookie {
	//nolint:gosec // Secure is deployment-configurable (COOKIE_SECURE); HttpOnly and SameSite are always set.
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
	}
}

func sessionTokenFromHeader(header http.Header) string {
	request := http.Request{Header: header}

	cookie, err := request.Cookie(sessionCookieName)
	if err != nil {
		return ""
	}

	return cookie.Value
}
