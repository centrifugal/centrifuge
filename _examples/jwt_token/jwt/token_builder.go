package jwt

import (
	"time"

	"github.com/cristalhq/jwt/v3"
)

func BuildUserToken(secret string, userID string, expireAt int64) (string, error) {
	key := []byte(secret)
	signer, _ := jwt.NewSignerHS(jwt.HS256, key)
	builder := jwt.NewBuilder(signer)
	claims := &connectTokenClaims{
		StandardClaims: jwt.StandardClaims{
			Subject: userID,
		},
	}
	if expireAt > 0 {
		claims.ExpiresAt = jwt.NewNumericDate(time.Unix(expireAt, 0))
	}
	token, err := builder.Build(claims)
	if err != nil {
		return "", err
	}
	return token.String(), nil
}
