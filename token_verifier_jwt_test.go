package centrifuge

import (
	"reflect"
	"testing"
	"time"
)

func Test_tokenVerifierJWT_Reload(t *testing.T) {
	type args struct {
		config Config
	}
	tests := []struct {
		name     string
		verifier TokenVerifier
		args     args
	}{
		{
			name:     "",
			verifier: nil,
			args:     args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//verifier := &tokenVerifierJWT{
			//	mu:                 tt.fields.mu,
			//	TokenHMACSecretKey: tt.fields.TokenHMACSecretKey,
			//	TokenRSAPublicKey:  tt.fields.TokenRSAPublicKey,
			//}
		})
	}
}

func Test_tokenVerifierJWT_VerifyConnectToken(t *testing.T) {
	type args struct {
		token string
	}
	verifierJWT := NewTokenVerifierJWT("secret", nil)
	_time := time.Now()
	tests := []struct {
		name      string
		verifier  TokenVerifier
		args      args
		want      ConnectToken
		wantErr   bool
		wantedErr *Error
	}{
		{
			name:     "Valid jwt",
			verifier: verifierJWT,
			args: args{
				token: getConnToken("user1", _time.Add(24*time.Hour).Unix()),
			},
			want: ConnectToken{
				UserID:   "user1",
				ExpireAt: _time.Add(24 * time.Hour).Unix(),
				Info:     nil,
			},
			wantErr: false,
		}, {
			name:     "Invalid jwt",
			verifier: verifierJWT,
			args: args{
				token: "Invalid jwt",
			},
			want:      ConnectToken{},
			wantErr:   true,
			wantedErr: ErrTokenInvalid,
		}, {
			name:     "Expired jwt",
			verifier: verifierJWT,
			args: args{
				token: getConnToken("user1", _time.Add(-24*time.Hour).Unix()),
			},
			want:      ConnectToken{},
			wantErr:   true,
			wantedErr: ErrorTokenExpired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.verifier.VerifyConnectToken(tt.args.token)
			if (err != nil) == tt.wantErr {
				if !reflect.DeepEqual(err, tt.wantedErr) {
					t.Errorf("VerifyConnectToken() error = %v, wantedErr %v", got, tt.wantedErr)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("VerifyConnectToken() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tokenVerifierJWT_VerifySubscribeToken(t *testing.T) {
	type args struct {
		token string
	}
	verifierJWT := NewTokenVerifierJWT("secret", nil)
	_time := time.Now()
	tests := []struct {
		name      string
		verifier  TokenVerifier
		args      args
		want      SubscribeToken
		wantErr   bool
		wantedErr *Error
	}{
		{
			name:      "Empty token",
			verifier:  verifierJWT,
			args:      args{},
			want:      SubscribeToken{},
			wantErr:   true,
			wantedErr: ErrTokenInvalid,
		}, {
			name:     "Invalid token",
			verifier: verifierJWT,
			args: args{
				token: "randomToken",
			},
			want:      SubscribeToken{},
			wantErr:   true,
			wantedErr: ErrTokenInvalid,
		}, {
			name:     "Expired token",
			verifier: verifierJWT,
			args: args{
				token: getSubscribeToken("channel1", "user1", _time.Add(-24*time.Hour).Unix()),
			},
			want:      SubscribeToken{},
			wantErr:   true,
			wantedErr: ErrorTokenExpired,
		}, {
			name:     "Valid token",
			verifier: verifierJWT,
			args: args{
				token: getSubscribeToken("channel1", "user1", _time.Add(24*time.Hour).Unix()),
			},
			want: SubscribeToken{
				UserID:   "user1",
				ExpireAt: _time.Add(24 * time.Hour).Unix(),
				Info:     nil,
				Channel:  "channel1",
			},
			wantErr:   true,
			wantedErr: ErrorTokenExpired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.verifier.VerifySubscribeToken(tt.args.token)
			if (err != nil) == tt.wantErr {
				if !reflect.DeepEqual(err, tt.wantedErr) {
					t.Errorf("VerifySubscribeToken() error = %v, wantedErr %v", err, tt.wantedErr)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("VerifySubscribeToken() got = %v, want %v", got, tt.want)
			}
		})
	}
}
