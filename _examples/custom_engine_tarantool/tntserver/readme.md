## Tarantool-based Centrifugo/Centrifuge Broker and PresenceManager

What's inside:

* PUB/SUB implementation to scale Centrifuge/Centrifugo nodes (PULL or PUSH based)
* Message history inside channels with retention (ttl and size) to survive mass reconnects and prevent message loss
* Presence support – to answer on a question who are the current subscribers of a certain channel

# The underlying spaces

The module creates several spaces internally.

* `pubs` to keep publication history for channels (for channels with history enabled)
* `meta` to keep history metadata (channel current max offset and epoch)
* `presence` to keep channel presence information (advice: make it temporary)

These spaces created automatically when you initialize the module by calling `centrifuge.init()`.
