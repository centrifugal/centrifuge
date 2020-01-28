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
		name     string
		verifier tokenVerifier
		args     args
		want     connectToken
		wantErr  bool
		expired  bool
	}{
		{
			name:     "Valid JWT",
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
			name:     "Invalid JWT",
			verifier: verifierJWT,
			args: args{
				token: "Invalid jwt",
			},
			want:    connectToken{},
			wantErr: true,
			expired: false,
		}, {
			name:     "Expired JWT",
			verifier: verifierJWT,
			args: args{
				token: getConnToken("user1", _time.Add(-24*time.Hour).Unix()),
			},
			want:    connectToken{},
			wantErr: true,
			expired: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.verifier.VerifyConnectToken(tt.args.token)
			if tt.wantErr && err == nil {
				t.Errorf("VerifyConnectToken() should return error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("VerifyConnectToken() should not return error")
			}
			if tt.expired && err != errTokenExpired {
				t.Errorf("VerifyConnectToken() should return token expired error")
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
		name     string
		verifier tokenVerifier
		args     args
		want     subscribeToken
		wantErr  bool
		expired  bool
	}{
		{
			name:     "Empty JWT",
			verifier: verifierJWT,
			args:     args{},
			want:     subscribeToken{},
			wantErr:  true,
			expired:  false,
		}, {
			name:     "Invalid JWT",
			verifier: verifierJWT,
			args: args{
				token: "randomToken",
			},
			want:    subscribeToken{},
			wantErr: true,
			expired: false,
		}, {
			name:     "Expired JWT",
			verifier: verifierJWT,
			args: args{
				token: getSubscribeToken("channel1", "client1", _time.Add(-24*time.Hour).Unix()),
			},
			want:    subscribeToken{},
			wantErr: true,
			expired: true,
		}, {
			name:     "Valid JWT",
			verifier: verifierJWT,
			args: args{
				token: getSubscribeToken("channel1", "client1", _time.Add(24*time.Hour).Unix()),
			},
			want: subscribeToken{
				Client:   "client1",
				ExpireAt: _time.Add(24 * time.Hour).Unix(),
				Info:     nil,
				Channel:  "channel1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.verifier.VerifySubscribeToken(tt.args.token)
			if tt.wantErr && err == nil {
				t.Errorf("VerifySubscribeToken() should return error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("VerifySubscribeToken() should not return error")
			}
			if tt.expired && err != errTokenExpired {
				t.Errorf("VerifySubscribeToken() should return token expired error")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("VerifySubscribeToken() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkConnectTokenVerify(b *testing.B) {
	verifierJWT := newTokenVerifierJWT("secret", nil)
	token := getConnToken("user1", time.Now().Add(24*time.Hour).Unix())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := verifierJWT.VerifyConnectToken(token)
		if err != nil {
			b.Fatal(errMalformedToken)
		}
	}
	b.StopTimer()
	b.ReportAllocs()
}
