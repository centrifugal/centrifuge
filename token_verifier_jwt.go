package centrifuge

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cristalhq/jwt"
)

type tokenVerifierJWT struct {
	mu        sync.RWMutex
	signer    *signer
	validator *jwt.Validator
}

// validAtNowChecker validates whether the token is valid at the current time, based on
// the values of the NotBefore and ExpiresAt claims.
// TODO: add IssuedAt check.
func validAtNowChecker() jwt.Check {
	return func(claims *jwt.StandardClaims) error {
		now := time.Now()
		if claims.IsExpired(now) || !claims.HasPassedNotBefore(now) {
			return jwt.ErrTokenExpired
		}
		return nil
	}
}

func newTokenVerifierJWT(tokenHMACSecretKey string, pubKey *rsa.PublicKey) tokenVerifier {
	verifier := &tokenVerifierJWT{
		validator: jwt.NewValidator(
			validAtNowChecker(),
		),
	}
	signer, err := newSigner(tokenHMACSecretKey, pubKey)
	if err != nil {
		panic(err)
	}
	verifier.signer = signer
	return verifier
}

var (
	errTokenExpired         = errors.New("token expired")
	errUnsupportedAlgorithm = errors.New("unsupported JWT algorithm")
	errDisabledAlgorithm    = errors.New("disabled JWT algorithm")
)

type connectTokenClaims struct {
	Info       json.RawMessage `json:"info,omitempty"`
	Base64Info string          `json:"b64info,omitempty"`
	Channels   []string        `json:"channels,omitempty"`
	jwt.StandardClaims
}

func (c *connectTokenClaims) MarshalBinary() ([]byte, error) {
	return json.Marshal(c)
}

type subscribeTokenClaims struct {
	Client          string          `json:"client,omitempty"`
	Channel         string          `json:"channel,omitempty"`
	Info            json.RawMessage `json:"info,omitempty"`
	Base64Info      string          `json:"b64info,omitempty"`
	ExpireTokenOnly bool            `json:"eto,omitempty"`
	jwt.StandardClaims
}

func (c *subscribeTokenClaims) MarshalBinary() ([]byte, error) {
	return json.Marshal(c)
}

type signer struct {
	HS256 jwt.Signer
	HS384 jwt.Signer
	HS512 jwt.Signer
	RS256 jwt.Signer
	RS384 jwt.Signer
	RS512 jwt.Signer
}

func newSigner(tokenHMACSecretKey string, pubKey *rsa.PublicKey) (*signer, error) {
	s := &signer{}

	// HMAC SHA.
	if tokenHMACSecretKey != "" {
		signerHS256, err := jwt.NewHS256([]byte(tokenHMACSecretKey))
		if err != nil {
			return nil, err
		}
		signerHS384, err := jwt.NewHS384([]byte(tokenHMACSecretKey))
		if err != nil {
			return nil, err
		}
		signerHS512, err := jwt.NewHS512([]byte(tokenHMACSecretKey))
		if err != nil {
			return nil, err
		}
		s.HS256 = signerHS256
		s.HS384 = signerHS384
		s.HS512 = signerHS512
	}

	// RSA.
	if pubKey != nil {
		// Since we only decode tokens we don't need Private key here.
		// Can not pass nil to jwt signer constructor since it requires non-nil Private Key.
		dummyPrivateKey := &rsa.PrivateKey{}

		signerRS256, err := jwt.NewRS256(pubKey, dummyPrivateKey)
		if err != nil {
			return nil, err
		}
		signerRS384, err := jwt.NewRS384(pubKey, dummyPrivateKey)
		if err != nil {
			return nil, err
		}
		signerRS512, err := jwt.NewRS512(pubKey, dummyPrivateKey)
		if err != nil {
			return nil, err
		}
		s.RS256 = signerRS256
		s.RS384 = signerRS384
		s.RS512 = signerRS512
	}

	return s, nil
}

func (s *signer) verify(token *jwt.Token) error {
	var signer jwt.Signer
	switch token.Header().Algorithm {
	case jwt.HS256:
		signer = s.HS256
	case jwt.HS384:
		signer = s.HS384
	case jwt.HS512:
		signer = s.HS512
	case jwt.RS256:
		signer = s.RS256
	case jwt.RS384:
		signer = s.RS384
	case jwt.RS512:
		signer = s.RS512
	default:
		return fmt.Errorf("%w: %s", errUnsupportedAlgorithm, string(token.Header().Algorithm))
	}
	if signer == nil {
		return fmt.Errorf("%w: %s", errDisabledAlgorithm, string(token.Header().Algorithm))
	}
	return signer.Verify(token.Payload(), token.Signature())
}

func (verifier *tokenVerifierJWT) verifySignature(token *jwt.Token) error {
	verifier.mu.RLock()
	defer verifier.mu.RUnlock()
	return verifier.signer.verify(token)
}

func (verifier *tokenVerifierJWT) VerifyConnectToken(t string) (connectToken, error) {
	token, err := jwt.Parse([]byte(t))
	if err != nil {
		return connectToken{}, err
	}

	err = verifier.verifySignature(token)
	if err != nil {
		return connectToken{}, err
	}

	claims := &connectTokenClaims{}
	err = json.Unmarshal(token.RawClaims(), claims)
	if err != nil {
		return connectToken{}, err
	}

	err = verifier.validator.Validate(&claims.StandardClaims)
	if err != nil {
		if err == jwt.ErrTokenExpired {
			return connectToken{}, errTokenExpired
		}
		return connectToken{}, err
	}

	ct := connectToken{
		UserID:   claims.StandardClaims.Subject,
		ExpireAt: int64(claims.StandardClaims.ExpiresAt),
		Info:     claims.Info,
		Channels: claims.Channels,
	}
	if claims.Base64Info != "" {
		byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
		if err != nil {
			return connectToken{}, err
		}
		ct.Info = byteInfo
	}
	return ct, nil
}

func (verifier *tokenVerifierJWT) VerifySubscribeToken(t string) (subscribeToken, error) {
	token, err := jwt.Parse([]byte(t))
	if err != nil {
		return subscribeToken{}, err
	}

	err = verifier.verifySignature(token)
	if err != nil {
		return subscribeToken{}, err
	}

	claims := &subscribeTokenClaims{}
	err = json.Unmarshal(token.RawClaims(), claims)
	if err != nil {
		return subscribeToken{}, err
	}

	err = verifier.validator.Validate(&claims.StandardClaims)
	if err != nil {
		if err == jwt.ErrTokenExpired {
			return subscribeToken{}, errTokenExpired
		}
		return subscribeToken{}, err
	}

	st := subscribeToken{
		Client:          claims.Client,
		Info:            claims.Info,
		Channel:         claims.Channel,
		ExpireAt:        int64(claims.StandardClaims.ExpiresAt),
		ExpireTokenOnly: claims.ExpireTokenOnly,
	}
	if claims.Base64Info != "" {
		byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
		if err != nil {
			return subscribeToken{}, err
		}
		st.Info = byteInfo
	}
	return st, nil
}

func (verifier *tokenVerifierJWT) Reload(config Config) error {
	verifier.mu.Lock()
	defer verifier.mu.Unlock()
	signer, err := newSigner(config.TokenHMACSecretKey, config.TokenRSAPublicKey)
	if err != nil {
		return err
	}
	verifier.signer = signer
	return nil
}
