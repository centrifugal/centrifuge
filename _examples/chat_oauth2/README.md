Simple chat with OAuth2 authentication via Google.

To start example run the following command from example directory:

```
GO111MODULE=on SESSION_SECRET=XXX GOOGLE_CLIENT_ID=XXX GOOGLE_CLIENT_SECRET=XXX go run main.go
```

To get GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET you need to register your application in Google Cloud Console (this is free). See instruction in [this great post by Henrik Fogelberg](https://medium.com/@hfogelberg/the-black-magic-of-oauth-in-golang-part-1-3cef05c28dde) on Medium.
