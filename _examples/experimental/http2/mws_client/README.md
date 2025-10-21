# MwsClient

```
brew install elixir
mix new mws_client
cd mws_client
mix deps.get
mix deps.compile
```

Then:

```
HTTP_MODE=h1 TARGET_HOST=localhost TARGET_PORT=8080 TARGET_PATH=/connection/websocket NUM=100 mix run -e "MwsClient.main()"
```

See connections to server:

```
netstat -an | grep '\.8080 .*ESTABLISHED' | \
awk '{if ($4 < $5) print $4" "$5; else print $5" "$4}' | sort -u | wc -l
```
