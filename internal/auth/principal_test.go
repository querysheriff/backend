package auth_test

import (
	"slices"
	"testing"

	"github.com/querysheriff/backend/internal/auth"
)

func TestAllowedServerFilterIsNilOnlyForSuperAdmins(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		principal *auth.Principal
		want      []string
	}{
		{"super admin", &auth.Principal{IsSuperAdmin: true, AllowedServers: []string{"prod"}}, nil},
		{"user", &auth.Principal{AllowedServers: []string{"prod"}}, []string{"prod"}},
		{"user without servers", &auth.Principal{}, []string{}},
		{"no principal", nil, []string{}},
	}

	for _, c := range cases {
		got := c.principal.AllowedServerFilter()
		if (got == nil) != (c.want == nil) || !slices.Equal(got, c.want) {
			t.Errorf("%s: AllowedServerFilter() = %#v, want %#v", c.name, got, c.want)
		}
	}
}
