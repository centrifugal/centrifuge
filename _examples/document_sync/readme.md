This example demonstrates a real-time document synchronization.

Note, this example assumes that conflicts are resolved during transaction on server side and only committed updates are sent in real-time. This still differs from CRDT approach and offline-first applications.

To achieve it we are using a helper class `RealTimeDocument` – we delegate Centrifuge Subscription to it, also a function to load the document and its version. Subscription channel must uniquely correlate with the document we want to sync.

We also provide a couple of additional functions to apply real-time updates to the current document version and compare versions (to ensure we are not applying non-actual real-time updates to the state).

To achieve the proper sync the version in the document should be incremental. In the real-world case this incremental version should be an incremental field in database. And we assume that instead of direct publishing to Centrifuge the backend is using transactional outbox or CDC approach – so that changes in document atomically saved and reliably exported to Centrifuge/Centrifugo.

Keep in mind, that messages must be sent to Centrifugo channel in the correct version order since we rely on version checks on the frontend.

The important thing here is that we should first subscribe to the channel, then load the document from the backend. This helps to not miss intermediary updates, happening between document load and subscription request. If we can guarantee that all updates eventually reach Centrifugo (and in case of transactional outbox or CDC we can) – then the sync will work properly.

Note that we send increments here, not counter value, and value is always properly synchronized. For the counter we could send just an actual value to the channel – we do not want making it here to demonstrate proper data sync.

To start the example run the following command from the example directory:

```
go run main.go
```

Then go to http://localhost:8000 to see it in action. Open a couple of browser windows. 
