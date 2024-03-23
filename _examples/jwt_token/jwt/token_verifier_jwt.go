package jwt

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cristalhq/jwt/v5"
)

type ConnectToken struct {
	// UserID tells library an ID of connecting user.
	UserID string
	// ExpireAt allows setting time in future when connection must be validated.
	// Validation can be server-side or client-side using Refresh handler.
	ExpireAt int64
	// Info contains additional information about connection. It will be
	// included into Join/Leave messages, into Presence information, also
	// info becomes a part of published message if it was published from
	// client directly. In some cases having additional info can be an
	// overhead – but you are simply free to not use it.
	Info []byte
	// Channels slice contains channels to subscribe connection to on server-side.
	Channels []string
}

type SubscribeToken struct {
	// Client is a unique client ID string set to each connection on server.
	// Will be compared with actual client ID.
	Client string
	// Channel client wants to subscribe. Will be compared with channel in
	// subscribe command.
	Channel string
	// ExpireAt allows setting time in future when connection must be validated.
	// Validation can be server-side or client-side using SubRefresh handler.
	ExpireAt int64
	// Info contains additional information about connection in channel.
	// It will be included into Join/Leave messages, into Presence information,
	// also channel info becomes a part of published message if it was published
	// from subscribed client directly.
	Info []byte
	// ExpireTokenOnly used to indicate that library must only check token
	// expiration but not turn on Subscription expiration checks on server side.
	// This allows to implement one-time subscription tokens.
	ExpireTokenOnly bool
}

type TokenVerifierConfig struct {
	// HMACSecretKey is a secret key used to validate connection and subscription
	// tokens generated using HMAC. Zero value means that HMAC tokens won't be allowed.
	HMACSecretKey string
	// RSAPublicKey is a public key used to validate connection and subscription
	// tokens generated using RSA. Zero value means that RSA tokens won't be allowed.
	RSAPublicKey *rsa.PublicKey
}

func NewTokenVerifier(config TokenVerifierConfig) *TokenVerifier {
	verifier := &TokenVerifier{}
	algorithms, err := newAlgorithms(config.HMACSecretKey, config.RSAPublicKey)
	if err != nil {
		panic(err)
	}
	verifier.algorithms = algorithms
	return verifier
}

type TokenVerifier struct {
	mu         sync.RWMutex
	algorithms *algorithms
}

var (
	ErrTokenExpired         = errors.New("token expired")
	errUnsupportedAlgorithm = errors.New("unsupported JWT algorithm")
	errDisabledAlgorithm    = errors.New("disabled JWT algorithm")
)

type connectTokenClaims struct {
	Info       json.RawMessage `json:"info,omitempty"`
	Base64Info string          `json:"b64info,omitempty"`
	Channels   []string        `json:"channels,omitempty"`
	jwt.RegisteredClaims
}

type subscribeTokenClaims struct {
	Client          string          `json:"client,omitempty"`
	Channel         string          `json:"channel,omitempty"`
	Info            json.RawMessage `json:"info,omitempty"`
	Base64Info      string          `json:"b64info,omitempty"`
	ExpireTokenOnly bool            `json:"eto,omitempty"`
	jwt.RegisteredClaims
}

type algorithms struct {
	HS256 jwt.Verifier
	HS384 jwt.Verifier
	HS512 jwt.Verifier
	RS256 jwt.Verifier
	RS384 jwt.Verifier
	RS512 jwt.Verifier
}

func newAlgorithms(tokenHMACSecretKey string, pubKey *rsa.PublicKey) (*algorithms, error) {
	alg := &algorithms{}

	// HMAC SHA.
	if tokenHMACSecretKey != "" {
		verifierHS256, err := jwt.NewVerifierHS(jwt.HS256, []byte(tokenHMACSecretKey))
		if err != nil {
			return nil, err
		}
		verifierHS384, err := jwt.NewVerifierHS(jwt.HS384, []byte(tokenHMACSecretKey))
		if err != nil {
			return nil, err
		}
		verifierHS512, err := jwt.NewVerifierHS(jwt.HS512, []byte(tokenHMACSecretKey))
		if err != nil {
			return nil, err
		}
		alg.HS256 = verifierHS256
		alg.HS384 = verifierHS384
		alg.HS512 = verifierHS512
	}

	// RSA.
	if pubKey != nil {
		verifierRS256, err := jwt.NewVerifierRS(jwt.RS256, pubKey)
		if err != nil {
			return nil, err
		}
		verifierRS384, err := jwt.NewVerifierRS(jwt.RS384, pubKey)
		if err != nil {
			return nil, err
		}
		verifierRS512, err := jwt.NewVerifierRS(jwt.RS512, pubKey)
		if err != nil {
			return nil, err
		}
		alg.RS256 = verifierRS256
		alg.RS384 = verifierRS384
		alg.RS512 = verifierRS512
	}

	return alg, nil
}

func (verifier *TokenVerifier) verifySignature(token *jwt.Token) error {
	verifier.mu.RLock()
	defer verifier.mu.RUnlock()

	var verifierFunc jwt.Verifier
	switch token.Header().Algorithm {
	case jwt.HS256:
		verifierFunc = verifier.algorithms.HS256
	case jwt.HS384:
		verifierFunc = verifier.algorithms.HS384
	case jwt.HS512:
		verifierFunc = verifier.algorithms.HS512
	case jwt.RS256:
		verifierFunc = verifier.algorithms.RS256
	case jwt.RS384:
		verifierFunc = verifier.algorithms.RS384
	case jwt.RS512:
		verifierFunc = verifier.algorithms.RS512
	default:
		return fmt.Errorf("%w: %s", errUnsupportedAlgorithm, string(token.Header().Algorithm))
	}

	if verifierFunc == nil {
		return fmt.Errorf("%w: %s", errDisabledAlgorithm, string(token.Header().Algorithm))
	}

	return verifierFunc.Verify(token)
}

func (verifier *TokenVerifier) VerifyConnectToken(t string) (ConnectToken, error) {
	token, err := jwt.ParseNoVerify([]byte(t))
	if err != nil {
		return ConnectToken{}, fmt.Errorf("error parsing connect token: %w", err)
	}

	err = verifier.verifySignature(token)
	if err != nil {
		return ConnectToken{}, fmt.Errorf("error verifying connect token signature: %w", err)
	}

	token, err = jwt.Parse([]byte(t), verifier.selectVerifier(token.Header().Algorithm))
	if err != nil {
		return ConnectToken{}, fmt.Errorf("error verifying connect token: %w", err)
	}

	claims := &connectTokenClaims{}
	err = json.Unmarshal(token.Claims(), claims)
	if err != nil {
		return ConnectToken{}, fmt.Errorf("error unmarshalling connect token claims: %w", err)
	}

	now := time.Now()
	if !claims.IsValidExpiresAt(now) {
		return ConnectToken{}, ErrTokenExpired
	}
	if !claims.IsValidNotBefore(now) {
		return ConnectToken{}, errors.New("token not valid yet")
	}

	ct := ConnectToken{
		UserID:   claims.RegisteredClaims.Subject,
		Info:     claims.Info,
		Channels: claims.Channels,
	}
	if claims.ExpiresAt != nil {
		ct.ExpireAt = claims.ExpiresAt.Unix()
	}
	if claims.Base64Info != "" {
		byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
		if err != nil {
			return ConnectToken{}, fmt.Errorf("error decoding base64 info in connect token: %w", err)
		}
		ct.Info = byteInfo
	}
	return ct, nil
}

func (verifier *TokenVerifier) selectVerifier(alg jwt.Algorithm) jwt.Verifier {
	verifier.mu.RLock()
	defer verifier.mu.RUnlock()

	switch alg {
	case jwt.HS256:
		return verifier.algorithms.HS256
	case jwt.HS384:
		return verifier.algorithms.HS384
	case jwt.HS512:
		return verifier.algorithms.HS512
	case jwt.RS256:
		return verifier.algorithms.RS256
	case jwt.RS384:
		return verifier.algorithms.RS384
	case jwt.RS512:
		return verifier.algorithms.RS512
	default:
		return nil
	}
}

func (verifier *TokenVerifier) VerifySubscribeToken(t string) (SubscribeToken, error) {
	token, err := jwt.ParseNoVerify([]byte(t))
	if err != nil {
		return SubscribeToken{}, err
	}

	err = verifier.verifySignature(token)
	if err != nil {
		return SubscribeToken{}, err
	}

	token, err = jwt.Parse([]byte(t), verifier.selectVerifier(token.Header().Algorithm))
	if err != nil {
		return SubscribeToken{}, fmt.Errorf("error verifying subscribe token: %w", err)
	}

	claims := &subscribeTokenClaims{}
	err = json.Unmarshal(token.Claims(), claims)
	if err != nil {
		return SubscribeToken{}, err
	}

	now := time.Now()
	if !claims.IsValidExpiresAt(now) {
		return SubscribeToken{}, ErrTokenExpired
	}
	if !claims.IsValidNotBefore(now) {
		return SubscribeToken{}, errors.New("token not valid yet")
	}

	st := SubscribeToken{
		Client:          claims.Client,
		Channel:         claims.Channel,
		ExpireAt:        claims.ExpiresAt.Unix(),
		ExpireTokenOnly: claims.ExpireTokenOnly,
	}

	// Decode the Info field if it's present
	if len(claims.Info) > 0 {
		st.Info = claims.Info
	} else if claims.Base64Info != "" {
		// If Info is not present, but Base64Info is, decode it
		byteInfo, err := base64.StdEncoding.DecodeString(claims.Base64Info)
		if err != nil {
			return SubscribeToken{}, fmt.Errorf("error decoding base64 info in subscribe token: %w", err)
		}
		st.Info = byteInfo
	}

	return st, nil
}

func (verifier *TokenVerifier) Reload(config TokenVerifierConfig) error {
	verifier.mu.Lock()
	defer verifier.mu.Unlock()
	alg, err := newAlgorithms(config.HMACSecretKey, config.RSAPublicKey)
	if err != nil {
		return err
	}
	verifier.algorithms = alg
	return nil
}
