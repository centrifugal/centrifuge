package centrifuge

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Use https://jwt.io to look at token contents.
//noinspection ALL
const (
	jwtValid            = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyNjk0IiwiaW5mbyI6eyJmaXJzdF9uYW1lIjoiQWxleGFuZGVyIiwibGFzdF9uYW1lIjoiRW1lbGluIn19.m-TaS80RxkAiP9jH_s_h2NrKS_TDuPxJ8-z6gI7UewI"
	jwtExpired          = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyNjk0IiwiaW5mbyI6eyJmaXJzdF9uYW1lIjoiQWxleGFuZGVyIiwibGFzdF9uYW1lIjoiRW1lbGluIn0sImV4cCI6MTU4ODM1MTcwNH0.LTc0p5YlrwJcxXPETrjhm9qyYUBKCR5fSROmfCE4TD8"
	jwtNotBefore        = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyNjk0IiwiaW5mbyI6eyJmaXJzdF9uYW1lIjoiQWxleGFuZGVyIiwibGFzdF9uYW1lIjoiRW1lbGluIn0sImV4cCI6MzE3NjgyMDU3MCwibmJmIjozMTc2ODIwNTYwfQ.gfsQeznFw6g44OEnCTSBW7AkmLy92GBfXL_Bdvzs7vc"
	jwtInvalidSignature = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyNjk0IiwiaW5mbyI6eyJmaXJzdF9uYW1lIjoiQWxleGFuZGVyIiwibGFzdF9uYW1lIjoiRW1lbGluIn0sImV4cCI6MTU4ODQxOTY5MywibmJmIjoxNTg4NDE4NjkzfQ.05Xj9adbLukdhSJFyiVUEgbxCHTajXuotmalFgYviCo"
	jwtArrayAud         = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyNjk0IiwiaW5mbyI6eyJmaXJzdF9uYW1lIjoiQWxleGFuZGVyIiwibGFzdF9uYW1lIjoiRW1lbGluIn0sImF1ZCI6WyJmb28iLCJiYXIiXX0.iY4pCPEQwstfNmPkLr7r7DrLZDo42q3E9jMc-TefI6g"
	jwtStringAud        = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyNjk0IiwiaW5mbyI6eyJmaXJzdF9uYW1lIjoiQWxleGFuZGVyIiwibGFzdF9uYW1lIjoiRW1lbGluIn0sImF1ZCI6ImZvbyJ9.jym6CG5haHME3ZQbb9jlnV1E0hSwwEjZycBZSygRzO0"
)

func Test_tokenVerifierJWT_Valid(t *testing.T) {
	verifier := newTokenVerifierJWT("secret", nil)
	ct, err := verifier.VerifyConnectToken(jwtValid)
	require.NoError(t, err)
	require.Equal(t, "2694", ct.UserID)
	require.NotNil(t, ct.Info)
	require.Equal(t, `{"first_name":"Alexander","last_name":"Emelin"}`, string(ct.Info))
}

func Test_tokenVerifierJWT_Expired(t *testing.T) {
	verifier := newTokenVerifierJWT("secret", nil)
	_, err := verifier.VerifyConnectToken(jwtExpired)
	require.Error(t, err)
	require.Equal(t, errTokenExpired, err)
}

func Test_tokenVerifierJWT_DisabledAlgorithm(t *testing.T) {
	verifier := newTokenVerifierJWT("", nil)
	_, err := verifier.VerifyConnectToken(jwtExpired)
	require.Error(t, err)
	require.True(t, errors.Is(err, errDisabledAlgorithm), err.Error())
}

func Test_tokenVerifierJWT_InvalidSignature(t *testing.T) {
	verifier := newTokenVerifierJWT("secret", nil)
	_, err := verifier.VerifyConnectToken(jwtInvalidSignature)
	require.Error(t, err)
}

func Test_tokenVerifierJWT_WithNotBefore(t *testing.T) {
	verifier := newTokenVerifierJWT("secret", nil)
	_, err := verifier.VerifyConnectToken(jwtNotBefore)
	require.Error(t, err)
}

func Test_tokenVerifierJWT_StringAudience(t *testing.T) {
	verifier := newTokenVerifierJWT("secret", nil)
	ct, err := verifier.VerifyConnectToken(jwtStringAud)
	require.NoError(t, err)
	require.Equal(t, "2694", ct.UserID)
}

func Test_tokenVerifierJWT_ArrayAudience(t *testing.T) {
	verifier := newTokenVerifierJWT("secret", nil)
	ct, err := verifier.VerifyConnectToken(jwtArrayAud)
	require.NoError(t, err)
	require.Equal(t, "2694", ct.UserID)
}

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

func BenchmarkConnectTokenVerify_Valid(b *testing.B) {
	verifierJWT := newTokenVerifierJWT("secret", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := verifierJWT.VerifyConnectToken(jwtValid)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkConnectTokenVerify_Expired(b *testing.B) {
	verifier := newTokenVerifierJWT("secret", nil)
	for i := 0; i < b.N; i++ {
		_, err := verifier.VerifyConnectToken(jwtExpired)
		if err != errTokenExpired {
			panic(err)
		}
	}
}
