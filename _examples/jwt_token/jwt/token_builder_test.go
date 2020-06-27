package jwt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildUserToken(t *testing.T) {
	token, err := BuildUserToken("secret", "42", 0)
	require.NoError(t, err)
	require.Equal(t, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0MiJ9.79CVgMIG_JB921dVUeJDWNJubJojFodH3-XtC_1C408", token)
}
