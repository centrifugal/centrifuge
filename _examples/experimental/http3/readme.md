The example demonstrates use of HTTP/3 server. Tested in Chrome only.

Requires valid certificates, see instructions in `certs` directory.

Run Chrome with `--origin-to-force-quic-on=localhost:4433` (required since we are using self-signed cert):

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --origin-to-force-quic-on=localhost:4433
```

Run and open https://localhost:4433 and look at network tab of devtools.
