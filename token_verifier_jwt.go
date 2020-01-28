package centrifuge

import (
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/dgrijalva/jwt-go"
)

type tokenVerifierJWT struct {
	mu                 sync.RWMutex
	TokenHMACSecretKey string
	TokenRSAPublicKey  *rsa.PublicKey
}

func newTokenVerifierJWT(tokenHMACSecretKey string, tokenRSAPublicKey *rsa.PublicKey) tokenVerifier {
	return &tokenVerifierJWT{
		TokenHMACSecretKey: tokenHMACSecretKey,
		TokenRSAPublicKey:  tokenRSAPublicKey,
	}
}

var (
	errTokenInvalid     = errors.New("invalid connection token")
	errTokenInvalidInfo = errors.New("can not decode provided info")
	errTokenExpired     = errors.New("token expired")
)

func (verifier *tokenVerifierJWT) VerifyConnectToken(token string) (connectToken, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &connectTokenClaims{}, verifier.jwtKeyFunc())
	if err != nil {
		if err, ok := err.(*jwt.ValidationError); ok {
			if err.Errors == jwt.ValidationErrorExpired {
				// The only problem with token is its expiration - no other
				// errors set in Errors bitfield.
				return connectToken{}, errTokenExpired
			}
		}
		return connectToken{}, errTokenInvalid
	}
	if claims, ok := parsedToken.Claims.(*connectTokenClaims); ok && parsedToken.Valid {
		token := connectToken{
			UserID:   claims.StandardClaims.Subject,
			ExpireAt: claims.StandardClaims.ExpiresAt,
			Info:     claims.Info,
		}
		if claims.Base64Info != "" {
			byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
			if err != nil {
				return connectToken{}, errTokenInvalidInfo
			}
			token.Info = byteInfo
		}
		return token, nil
	}
	return connectToken{}, errTokenInvalid
}

func (verifier *tokenVerifierJWT) VerifySubscribeToken(token string) (subscribeToken, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &subscribeTokenClaims{}, verifier.jwtKeyFunc())
	if err != nil {
		if validationErr, ok := err.(*jwt.ValidationError); ok {
			if validationErr.Errors == jwt.ValidationErrorExpired {
				// The only problem with token is its expiration - no other
				// errors set in Errors bitfield.
				return subscribeToken{}, errTokenExpired
			}
		}
		return subscribeToken{}, errTokenInvalid
	}
	if claims, ok := parsedToken.Claims.(*subscribeTokenClaims); ok && parsedToken.Valid {
		token := subscribeToken{
			Client:   claims.Client,
			Info:     claims.Info,
			Channel:  claims.Channel,
			ExpireAt: claims.StandardClaims.ExpiresAt,
		}
		if claims.Base64Info != "" {
			byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
			if err != nil {
				return subscribeToken{}, errTokenInvalidInfo
			}
			token.Info = byteInfo
		}
		return token, nil
	}
	return subscribeToken{}, errTokenInvalid
}

func (verifier *tokenVerifierJWT) Reload(config Config) {
	verifier.mu.Lock()
	defer verifier.mu.Unlock()
	verifier.TokenRSAPublicKey = config.TokenRSAPublicKey
	verifier.TokenHMACSecretKey = config.TokenHMACSecretKey
}

func (verifier *tokenVerifierJWT) jwtKeyFunc() func(token *jwt.Token) (interface{}, error) {
	return func(token *jwt.Token) (interface{}, error) {
		verifier.mu.RLock()
		defer verifier.mu.RUnlock()
		switch token.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if verifier.TokenHMACSecretKey == "" {
				return nil, fmt.Errorf("token HMAC secret key not set")
			}
			return []byte(verifier.TokenHMACSecretKey), nil
		case *jwt.SigningMethodRSA:
			if verifier.TokenRSAPublicKey == nil {
				return nil, fmt.Errorf("token RSA public key not set")
			}
			return verifier.TokenRSAPublicKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing method: %v", token.Header["alg"])
		}
	}
}
