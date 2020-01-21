package centrifuge

import (
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"sync"
)

var (
	InvalidToken = errors.New("invalid connection token")
	InvalidInfo  = errors.New("can not decode provided info from base64")
)

type AuthorizationJwt struct {
	mu                 sync.RWMutex
	TokenHMACSecretKey string
	TokenRSAPublicKey  *rsa.PublicKey
}

func NewAuthorizationJwt(tokenHMACSecretKey string, tokenRSAPublicKey *rsa.PublicKey) Authorization {
	return &AuthorizationJwt{
		TokenHMACSecretKey: tokenHMACSecretKey,
		TokenRSAPublicKey:  tokenRSAPublicKey,
	}
}

func (auth *AuthorizationJwt) VerifyConnectToken(token string) (Token, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &connectTokenClaims{}, auth.jwtKeyFunc())
	if err != nil {
		if err, ok := err.(*jwt.ValidationError); ok {
			if err.Errors == jwt.ValidationErrorExpired {
				// The only problem with token is its expiration - no other
				// errors set in Errors bitfield.
				return Token{}, ErrorTokenExpired
			}
		}
		return Token{}, InvalidToken
	}
	if claims, ok := parsedToken.Claims.(*connectTokenClaims); ok && parsedToken.Valid {
		token := Token{
			UserID:   claims.StandardClaims.Subject,
			ExpireAt: claims.StandardClaims.ExpiresAt,
			Info:     claims.Info,
		}
		if claims.Base64Info != "" {
			byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
			if err != nil {
				return Token{}, InvalidInfo
			}
			token.Info = byteInfo
		}
		return token, nil
	}
	return Token{}, InvalidToken
}

func (auth *AuthorizationJwt) VerifySubscribeToken(token string) (Token, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &subscribeTokenClaims{}, auth.jwtKeyFunc())
	if err != nil {
		if validationErr, ok := err.(*jwt.ValidationError); ok {
			if validationErr.Errors == jwt.ValidationErrorExpired {
				// The only problem with token is its expiration - no other
				// errors set in Errors bitfield.
				return Token{}, ErrorTokenExpired
			}
		}
		return Token{}, InvalidToken
	}
	if claims, ok := parsedToken.Claims.(*subscribeTokenClaims); ok && parsedToken.Valid {
		token := Token{
			UserID:   claims.Client,
			Info:     claims.Info,
			Channel:  claims.Channel,
			ExpireAt: claims.StandardClaims.ExpiresAt,
		}
		if claims.Base64Info != "" {
			byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
			if err != nil {
				return Token{}, InvalidInfo
			}
			token.Info = byteInfo
		}
		return token, nil
	}
	return Token{}, InvalidToken
}

func (auth *AuthorizationJwt) Reload(config Config) {
	auth.mu.RLock()
	defer auth.mu.RUnlock()
	auth.TokenRSAPublicKey = config.TokenRSAPublicKey
	auth.TokenHMACSecretKey = config.TokenHMACSecretKey
}

func (auth *AuthorizationJwt) jwtKeyFunc() func(token *jwt.Token) (interface{}, error) {
	return func(token *jwt.Token) (interface{}, error) {
		switch token.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if auth.TokenHMACSecretKey == "" {
				return nil, fmt.Errorf("token HMAC secret key not set")
			}
			return []byte(auth.TokenHMACSecretKey), nil
		case *jwt.SigningMethodRSA:
			if auth.TokenRSAPublicKey == nil {
				return nil, fmt.Errorf("token RSA public key not set")
			}
			return auth.TokenRSAPublicKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing method: %v", token.Header["alg"])
		}
	}
}
