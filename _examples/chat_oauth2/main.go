package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/dchest/uniuri"
	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// SessionName is the key used to access the session store.
const SessionName = "_session"

// Store can/should be set by applications. The default is a cookie store.
var Store sessions.Store

func init() {
	if os.Getenv("SESSION_SECRET") == "" {
		log.Fatal("SESSION_SECRET environment variable required")
	}
	if os.Getenv("GOOGLE_CLIENT_ID") == "" {
		log.Fatal("GOOGLE_CLIENT_ID environment variable required")
	}
	if os.Getenv("GOOGLE_CLIENT_SECRET") == "" {
		log.Fatal("GOOGLE_CLIENT_SECRET environment variable required")
	}
	key := []byte(os.Getenv("SESSION_SECRET"))
	cookieStore := sessions.NewCookieStore(key)
	cookieStore.Options.HttpOnly = true
	cookieStore.Options.MaxAge = 3600
	Store = cookieStore
}

var googleOauthConfig = &oauth2.Config{
	RedirectURL:  "http://localhost:3000/callback",
	ClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
	ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
	Scopes: []string{
		"https://www.googleapis.com/auth/userinfo.profile",
		"https://www.googleapis.com/auth/userinfo.email"},
	Endpoint: google.Endpoint,
}

// GoogleUser ...
type GoogleUser struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	VerifiedEmail bool   `json:"verified_email"`
	Name          string `json:"name"`
	GivenName     string `json:"given_name"`
	FamilyName    string `json:"family_name"`
	Link          string `json:"link"`
	Picture       string `json:"picture"`
	Gender        string `json:"gender"`
	Locale        string `json:"locale"`
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, _ := GetFromSession("user", r)
		if value == "" {
			http.Redirect(w, r, "/account", http.StatusTemporaryRedirect)
			return
		}
		var user *GoogleUser
		err := json.Unmarshal([]byte(value), &user)
		if err != nil {
			log.Printf("Error unmarshaling Google user %s\n", err.Error())
			_ = Logout(w, r)
			http.Redirect(w, r, "/account", http.StatusTemporaryRedirect)
			return
		}
		ctx := r.Context()
		ctx = centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: user.Email,
		})
		r = r.WithContext(ctx)
		h.ServeHTTP(w, r)
	})
}

func createCentrifugeNode() (*centrifuge.Node, error) {
	node, err := centrifuge.New(centrifuge.Config{})
	if err != nil {
		return nil, err
	}

	node.OnConnect(func(client *centrifuge.Client) {
		log.Printf("client %s connected via %s", client.UserID(), client.Transport().Name())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("client %s subscribes on channel %s", client.UserID(), e.Channel)
			cb(centrifuge.SubscribeReply{}, nil)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("client %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			cb(centrifuge.PublishReply{}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("client %s disconnected", client.UserID())
		})
	})

	if err := node.Run(); err != nil {
		return nil, err
	}

	return node, nil
}

func main() {
	node, err := createCentrifugeNode()
	if err != nil {
		log.Fatal(err)
	}

	router := mux.NewRouter().StrictSlash(true)

	// Chat handlers.
	router.Handle("/", authMiddleware(http.FileServer(http.Dir("./"))))
	router.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))

	// Auth handlers.
	router.HandleFunc("/account", accountHandler)
	router.HandleFunc("/login", loginHandler)
	router.HandleFunc("/logout", logoutHandler)
	router.HandleFunc("/callback", callbackHandler)

	if err := http.ListenAndServe(":3000", router); err != nil {
		log.Fatalln(err)
	}
}

func accountHandler(w http.ResponseWriter, r *http.Request) {
	value, _ := GetFromSession("user", r)
	if value != "" {
		_, _ = fmt.Fprintln(w, "<a href='/logout'>Logout</a>")
		return
	}
	_, _ = fmt.Fprintln(w, "<a href='/login'>Log in with Google</a>")
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	oauthStateString := uniuri.New()
	rawURL := googleOauthConfig.AuthCodeURL(oauthStateString)
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		log.Printf("error parsing url: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	state := parsedURL.Query().Get("state")
	err = StoreInSession("state", state, r, w)
	if err != nil {
		log.Printf("error storing in session %s\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, rawURL, http.StatusTemporaryRedirect)
}

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	_ = Logout(w, r)
	http.Redirect(w, r, "/account", http.StatusTemporaryRedirect)
}

func callbackHandler(w http.ResponseWriter, r *http.Request) {

	state := r.FormValue("state")
	if state == "" {
		_, _ = fmt.Fprintf(w, "Empty state received")
		return
	}

	savedState, _ := GetFromSession("state", r)
	if savedState == "" {
		_, _ = fmt.Fprintf(w, "No state in session")
		return
	}

	if state != savedState {
		_, _ = fmt.Fprintf(w, "Invalid state")
		return
	}

	code := r.FormValue("code")
	token, err := googleOauthConfig.Exchange(context.Background(), code)
	if err != nil {
		_, _ = fmt.Fprintf(w, "Code exchange failed with error %s\n", err.Error())
		return
	}

	if !token.Valid() {
		_, _ = fmt.Fprintln(w, "Retrieved invalid token")
		return
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	response, err := client.Get("https://www.googleapis.com/oauth2/v2/userinfo?access_token=" + token.AccessToken)
	if err != nil {
		log.Printf("Error getting user from token %s\n", err.Error())
		return
	}
	defer func() { _ = response.Body.Close() }()

	contents, err := io.ReadAll(response.Body)
	if err != nil {
		log.Printf("Error reading body  %s\n", err.Error())
		return
	}

	var user *GoogleUser
	err = json.Unmarshal(contents, &user)
	if err != nil {
		log.Printf("Error unmarshaling Google user %s\n", err.Error())
		return
	}

	err = StoreInSession("user", string(contents), r, w)
	if err != nil {
		log.Printf("Error storing in session %s\n", err.Error())
		return
	}

	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

// StoreInSession stores a specified key/value pair in the session.
func StoreInSession(key string, value string, req *http.Request, res http.ResponseWriter) error {
	session, _ := Store.New(req, SessionName)
	if err := updateSessionValue(session, key, value); err != nil {
		return err
	}
	return session.Save(req, res)
}

// GetFromSession retrieves a previously-stored value from the session.
// If no value has previously been stored at the specified key, it will return an error.
func GetFromSession(key string, req *http.Request) (string, error) {
	session, _ := Store.Get(req, SessionName)
	value, err := getSessionValue(session, key)
	if err != nil {
		return "", errors.New("could not find a matching session for this request")
	}
	return value, nil
}

func getSessionValue(session *sessions.Session, key string) (string, error) {
	value := session.Values[key]
	if value == nil {
		return "", fmt.Errorf("could not find a matching session for this request")
	}

	rdata := strings.NewReader(value.(string))
	r, err := gzip.NewReader(rdata)
	if err != nil {
		return "", err
	}
	s, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}

	return string(s), nil
}

func updateSessionValue(session *sessions.Session, key, value string) error {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write([]byte(value)); err != nil {
		return err
	}
	if err := gz.Flush(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}

	session.Values[key] = b.String()
	return nil
}

// Logout invalidates a user session.
func Logout(res http.ResponseWriter, req *http.Request) error {
	session, err := Store.Get(req, SessionName)
	if err != nil {
		return err
	}
	session.Options.MaxAge = -1
	session.Values = make(map[any]any)
	err = session.Save(req, res)
	if err != nil {
		return errors.New("could not delete user session")
	}
	return nil
}
