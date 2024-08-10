This example shows how to use custom engine implementation based on [Tarantool](https://www.tarantool.io/en/): i.e. it provides `Broker` and `PresenceManager`.

Tarantool provides faster history and presence operations than Redis (up to 10x), while being on pair in subscribe and publish performance.

**Important limitation to know**: Tarantool Broker uses channels that start with `__` (two underscores) for internal needs and does not allow subscribing on them from the outside.

Tarantool Broker PUB/SUB can be customized to work over PUSH or POLL.

**Since this is just an example – we do not guarantee any stability here**. This implementation have not been tested in production environment.

## Single Tarantool instance example

Go to `tntserver` dir and install `indexpiration` rock dependency:

```
tarantoolctl rocks install https://raw.githubusercontent.com/moonlibs/indexpiration/master/rockspecs/indexpiration-scm-1.rockspec
```

Create a directory for Tarantool `snap` and `xlog` files:

```
mkdir tnt1
```

Start Tarantool:

```
tarantool init.lua 1
```

In another terminal start chat application running the following command from example directory:

```
go run main.go
```

Go to http://localhost:8000. You will see simple chat app, try writing a chat message in one browser tab – you should see it appears in another tab.

## Client-side sharding

Go to `tntserver` dir and create directories for Tarantool files:

```
mkdir tnt1
mkdir tnt2
```

Start first Tarantool instance:

```
tarantool init.lua 1
```

Then second one:

```
tarantool init.lua 2
```

Now run first application instance on port 8000:

```
go run main.go --port 8000 --sharded
```

The second one on port 8001:

```
go run main.go --port 8001 --sharded
```

Go to http://localhost:8000 or http://localhost:8001 – nodes will be connected over Tarantool and data is consistently sharded between 2 different Tarantool instances (by a channel).

## Tarantool high availability example

Run Tarantool with leader-follower setup with Cartridge (using `127.0.0.1:3301` and `127.0.0.1:3302`). Then start application example:

```
go run main.go --port 8000 --ha
```

## Tarantool Raft high availability example

**Requires Tarantool 2.6.1+ since example uses Raft-based replication**.

Create directories for Tarantool files inside `tntserver` folder:

```
mkdir ha_tnt1 ha_tnt2 ha_tnt3
```

Run Tarantool cluster:

```
docker-compose up
```

Then start application:

```
go run main.go --port 8000 --ha --raft
```

At this moment you can temporary stop/run one of Tarantool instances using:

```
docker-compose pause tnt1
docker-compose unpause tnt1
```

See that chat application continues to work after a short downtime.
