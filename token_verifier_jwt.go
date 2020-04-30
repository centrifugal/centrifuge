package centrifuge

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dgrijalva/jwt-go/v4"
)

type tokenVerifierJWT struct {
	mu                 sync.RWMutex
	TokenHMACSecretKey []byte
	TokenRSAPublicKey  *rsa.PublicKey
}

func newTokenVerifierJWT(tokenHMACSecretKey string, tokenRSAPublicKey *rsa.PublicKey) tokenVerifier {
	return &tokenVerifierJWT{
		TokenHMACSecretKey: []byte(tokenHMACSecretKey),
		TokenRSAPublicKey:  tokenRSAPublicKey,
	}
}

var (
	errTokenExpired   = errors.New("token expired")
	errMalformedToken = errors.New("malformed token")
)

type connectTokenClaims struct {
	Info       json.RawMessage `json:"info"`
	Base64Info string          `json:"b64info"`
	Channels   []string        `json:"channels"`
	jwt.StandardClaims
}

type subscribeTokenClaims struct {
	Client          string          `json:"client"`
	Channel         string          `json:"channel"`
	Info            json.RawMessage `json:"info"`
	Base64Info      string          `json:"b64info"`
	ExpireTokenOnly bool            `json:"eto"`
	jwt.StandardClaims
}

func (verifier *tokenVerifierJWT) VerifyConnectToken(token string) (connectToken, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &connectTokenClaims{}, verifier.jwtKeyFunc())
	if err != nil {
		if _, ok := err.(*jwt.TokenExpiredError); ok {
			// The only problem with token is its expiration - no other
			// errors set in Errors bitfield.
			return connectToken{}, errTokenExpired
		}
		return connectToken{}, err
	}
	if claims, ok := parsedToken.Claims.(*connectTokenClaims); ok && parsedToken.Valid {
		token := connectToken{
			UserID:   claims.StandardClaims.Subject,
			Info:     claims.Info,
			Channels: claims.Channels,
		}
		if claims.StandardClaims.ExpiresAt != nil {
			token.ExpireAt = claims.StandardClaims.ExpiresAt.Unix()
		}
		if claims.Base64Info != "" {
			byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
			if err != nil {
				return connectToken{}, err
			}
			token.Info = byteInfo
		}
		return token, nil
	}
	return connectToken{}, errMalformedToken
}

func (verifier *tokenVerifierJWT) VerifySubscribeToken(token string) (subscribeToken, error) {
	parsedToken, err := jwt.ParseWithClaims(token, &subscribeTokenClaims{}, verifier.jwtKeyFunc())
	if err != nil {
		if _, ok := err.(*jwt.TokenExpiredError); ok {
			// The only problem with token is its expiration - no other
			// errors set in Errors bitfield.
			return subscribeToken{}, errTokenExpired
		}
		return subscribeToken{}, err
	}
	if claims, ok := parsedToken.Claims.(*subscribeTokenClaims); ok && parsedToken.Valid {
		token := subscribeToken{
			Client:          claims.Client,
			Info:            claims.Info,
			Channel:         claims.Channel,
			ExpireTokenOnly: claims.ExpireTokenOnly,
		}
		if claims.StandardClaims.ExpiresAt != nil {
			token.ExpireAt = claims.StandardClaims.ExpiresAt.Unix()
		}
		if claims.Base64Info != "" {
			byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
			if err != nil {
				return subscribeToken{}, err
			}
			token.Info = byteInfo
		}
		return token, nil
	}
	return subscribeToken{}, errMalformedToken
}

func (verifier *tokenVerifierJWT) Reload(config Config) {
	verifier.mu.Lock()
	defer verifier.mu.Unlock()
	verifier.TokenRSAPublicKey = config.TokenRSAPublicKey
	verifier.TokenHMACSecretKey = []byte(config.TokenHMACSecretKey)
}

func (verifier *tokenVerifierJWT) jwtKeyFunc() func(token *jwt.Token) (interface{}, error) {
	return func(token *jwt.Token) (interface{}, error) {
		verifier.mu.RLock()
		defer verifier.mu.RUnlock()
		switch token.Method.(type) {
		case *jwt.SigningMethodHMAC:
			if len(verifier.TokenHMACSecretKey) == 0 {
				return nil, fmt.Errorf("token HMAC secret key not set")
			}
			return verifier.TokenHMACSecretKey, nil
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
