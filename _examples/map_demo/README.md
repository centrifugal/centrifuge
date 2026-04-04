# Map Subscriptions Demo

This demo showcases Centrifuge's map subscriptions feature with multiple interactive examples: collaborative cursors, game lobby, inventory management, stock tickers, live scoreboard, and a data visualizer.

## Prerequisites

- Go 1.21+
- Redis (optional, for Redis backend)

## Quick Start

### In-Memory Backend (default)

```bash
go build -o map_demo . && ./map_demo
```

No external dependencies. Data is lost on restart.

### Redis Backend

```bash
docker-compose up -d
go build -o map_demo . && ./map_demo -redis "localhost:6379"
```

Open http://localhost:3000 in your browser.

## Command-Line Flags

| Flag    | Description      | Default |
|---------|------------------|---------|
| `-port` | HTTP server port | `3000`  |
| `-redis`| Redis address    | (none)  |
