v0.5.0
======

This release introduces server-side subscription support - see [#89](https://github.com/centrifugal/centrifuge/pull/89) for details. Release highlights:

* New field `Subscriptions` for `ConnectReply` to provide server side subscriptions
* New `Client.Subscribe` method
* New node configuration options: `UserSubscribePersonal` and `UserPersonalChannelPrefix`
* New `ServerSide` boolean namespace option
* Method `Client.Unsubscribe` now accepts one main argument (`channel`) and `UnsubscribeOptions`
* New option `UseWriteBufferPool` for WebSocket handler config
* Internal refactor of JWT related code, many thanks to [@Nesty92](https://github.com/Nesty92)
* Introduce subscription dissolver - node now reliably unsubscribes from PUB/SUB channels, see details in [#77](https://github.com/centrifugal/centrifuge/issues/77)

v0.4.0
======

* `Secret` config option renamed to `TokenHMACSecretKey`, that was reasonable due to recent addition of RSA-based tokens so we now have `TokenRSAPublicKey` option
