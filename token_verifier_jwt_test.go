package centrifuge

import (
	"reflect"
	"testing"
	"time"
)

func Test_tokenVerifierJWT_VerifyConnectToken(t *testing.T) {
	type args struct {
		token string
	}
	verifierJWT := newTokenVerifierJWT("secret", nil)
	_time := time.Now()
	tests := []struct {
		name      string
		verifier  tokenVerifier
		args      args
		want      connectToken
		wantErr   bool
		wantedErr error
	}{
		{
			name:     "Valid jwt",
			verifier: verifierJWT,
			args: args{
				token: getConnToken("user1", _time.Add(24*time.Hour).Unix()),
			},
			want: connectToken{
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
			want:      connectToken{},
			wantErr:   true,
			wantedErr: errTokenInvalid,
		}, {
			name:     "Expired jwt",
			verifier: verifierJWT,
			args: args{
				token: getConnToken("user1", _time.Add(-24*time.Hour).Unix()),
			},
			want:      connectToken{},
			wantErr:   true,
			wantedErr: errTokenExpired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.verifier.VerifyConnectToken(tt.args.token)
			if err != nil && tt.wantErr {
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
	verifierJWT := newTokenVerifierJWT("secret", nil)
	_time := time.Now()
	tests := []struct {
		name      string
		verifier  tokenVerifier
		args      args
		want      subscribeToken
		wantErr   bool
		wantedErr error
	}{
		{
			name:      "Empty token",
			verifier:  verifierJWT,
			args:      args{},
			want:      subscribeToken{},
			wantErr:   true,
			wantedErr: errTokenInvalid,
		}, {
			name:     "Invalid token",
			verifier: verifierJWT,
			args: args{
				token: "randomToken",
			},
			want:      subscribeToken{},
			wantErr:   true,
			wantedErr: errTokenInvalid,
		}, {
			name:     "Expired token",
			verifier: verifierJWT,
			args: args{
				token: getSubscribeToken("channel1", "user1", _time.Add(-24*time.Hour).Unix()),
			},
			want:      subscribeToken{},
			wantErr:   true,
			wantedErr: errTokenExpired,
		}, {
			name:     "Valid token",
			verifier: verifierJWT,
			args: args{
				token: getSubscribeToken("channel1", "user1", _time.Add(24*time.Hour).Unix()),
			},
			want: subscribeToken{
				Client:   "user1",
				ExpireAt: _time.Add(24 * time.Hour).Unix(),
				Info:     nil,
				Channel:  "channel1",
			},
			wantErr:   true,
			wantedErr: errTokenExpired,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.verifier.VerifySubscribeToken(tt.args.token)
			if err != nil && tt.wantErr {
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
