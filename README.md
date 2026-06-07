# chat-state-better-sqlite3

Community [`better-sqlite3`](https://www.npmjs.com/package/better-sqlite3) state adapter for [Chat SDK](https://chat-sdk.dev). Use this when you want persistent Chat SDK state in a local SQLite database without Redis, Postgres, MySQL, or a platform-specific runtime.

## Installation

```bash
npm install chat chat-state-better-sqlite3
```

Requires Node.js 20, 22, 23, 24, 25, or 26.

## Usage

```typescript
import { Chat } from "chat";
import { createSlackAdapter } from "@chat-adapter/slack";
import { createBetterSqlite3State } from "chat-state-better-sqlite3";

const bot = new Chat({
  userName: "my-bot",
  adapters: {
    slack: createSlackAdapter(),
  },
  state: createBetterSqlite3State({
    path: "./chat-state.sqlite",
  }),
});
```

`createBetterSqlite3State()` also reads `CHAT_STATE_SQLITE_PATH` or `SQLITE_PATH`. When no path is provided, it creates `./chat-state.sqlite`.

### Using an existing database

```typescript
import Database from "better-sqlite3";
import { createBetterSqlite3State } from "chat-state-better-sqlite3";

const database = new Database("./app.sqlite");

const state = createBetterSqlite3State({
  database,
  keyPrefix: "my-bot",
});
```

## Configuration

| Option | Required | Description |
| --- | --- | --- |
| `path` | No | SQLite database path. Defaults to `CHAT_STATE_SQLITE_PATH`, `SQLITE_PATH`, or `./chat-state.sqlite`. |
| `database` | No | Existing `better-sqlite3` database instance. |
| `databaseOptions` | No | Options passed to `new Database(path, options)`. |
| `keyPrefix` | No | Prefix for all state rows. Defaults to `"chat-sdk"`. |
| `logger` | No | Chat SDK logger instance. Defaults to `ConsoleLogger("info").child("better-sqlite3")`. |
| `closeOnDisconnect` | No | Close an existing database on `disconnect()`. Defaults to `false` for existing databases and `true` for owned path databases. |
| `enableWal` | No | Enable SQLite WAL mode for owned file databases. Defaults to `true`. |

Provide either `path` or `database`, not both.

## Data model

The adapter creates these tables automatically on `connect()`:

```sql
chat_state_subscriptions
chat_state_locks
chat_state_cache
chat_state_lists
chat_state_queues
```

All rows are namespaced by `key_prefix`, so multiple bots can share a database safely when they use different prefixes.

## Features

| Feature | Supported |
| --- | --- |
| Persistence | Yes |
| Subscriptions | Yes |
| Token-owned locks with TTL | Yes |
| Force lock release | Yes |
| Key-value cache with TTL | Yes |
| Atomic list append and trim | Yes |
| FIFO queue with TTL and max size | Yes |
| Automatic table creation | Yes |
| Key prefix namespacing | Yes |

## Adapter compatibility

Chat SDK separates platform adapters from state adapters. This package implements `StateAdapter`, so it can be used with Slack, Discord, Telegram, GitHub, Linear, Google Chat, Teams, WhatsApp, Matrix, Webex, email, iMessage, or any other Chat SDK platform adapter.

SQLite is still an embedded database. It is a good fit for one local process, small deployments, development tools, and desktop/server apps. For high-contention multi-region bots, use a networked state backend such as Redis, Postgres, MySQL, or Durable Objects.

## License

MIT
