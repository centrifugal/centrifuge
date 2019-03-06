In this example we show how to connect several Centrifuge server nodes with Redis engine.

To start install dependencies first:

```
dep ensure
```

Then start Nats server locally:

```
gnatsd
```

Then go to http://localhost:8000. You will see simple chat app, try writing a chat message in one browser tab and you should see it appears in another tab.
