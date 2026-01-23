# Presence as First-Class Subscription (Updated Design)

## Core Principle

**Presence is an independent, first-class keyed subscription associated with a channel.**

Presence:
- Does **not** depend on a data subscription
- Has its **own lifecycle**
- Can be consumed **without** subscribing to channel publications
- Reuses the channel namespace, but not the channel subscription state

Presence is conceptually a **keyed stream of active connections**.

---

## Client API

### Presence-Only Subscription

```javascript
const presence = centrifuge.newPresenceSubscription('chat:lobby');

presence.on('subscribed', (ctx) => {
    console.log('Online:', ctx.publications.length);
});

presence.on('publication', (ctx) => {
    if (ctx.data.removed) {
        console.log('Left:', ctx.data.key);
    } else {
        const info = JSON.parse(ctx.data.data);
        console.log('Joined:', info.userID);
    }
});

presence.subscribe();
```


Presence has its own permission callback.

```
client.OnPresenceSubscribe(func(
    ctx context.Context,
    e PresenceSubscribeEvent,
) (PresenceSubscribeReply, error) {
    // Example: allow everyone to observe presence
    return PresenceSubscribeReply{
        Allowed: true,
    }, nil
})
```