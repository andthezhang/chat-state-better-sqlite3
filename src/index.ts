import DatabaseConstructor from "better-sqlite3";
import type {
  Database as BetterSqlite3Database,
  Options as BetterSqlite3Options,
} from "better-sqlite3";
import { ConsoleLogger, Message } from "chat";
import type { Lock, Logger, QueueEntry, StateAdapter } from "chat";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

export interface BetterSqlite3StateAdapterOptions {
  /** SQLite database path. Defaults to ./chat-state.sqlite in the factory. */
  path: string;
  /** Options passed to better-sqlite3 when this adapter owns the database. */
  databaseOptions?: BetterSqlite3Options | undefined;
  /** Key prefix for all rows. */
  keyPrefix?: string | undefined;
  /** Logger instance for error reporting. */
  logger?: Logger | undefined;
  /** Close the owned database on disconnect. Defaults to true. */
  closeOnDisconnect?: boolean | undefined;
  /** Enable WAL mode for owned file databases. Defaults to true. */
  enableWal?: boolean | undefined;
}

export interface BetterSqlite3StateClientOptions {
  /** Existing better-sqlite3 database instance. */
  database: BetterSqlite3Database;
  /** Key prefix for all rows. */
  keyPrefix?: string | undefined;
  /** Logger instance for error reporting. */
  logger?: Logger | undefined;
  /** Close the existing database on disconnect. Defaults to false. */
  closeOnDisconnect?: boolean | undefined;
  /** Enable WAL mode on connect. Defaults to false for existing databases. */
  enableWal?: boolean | undefined;
}

export type CreateBetterSqlite3StateOptions =
  | (Partial<BetterSqlite3StateAdapterOptions> & { database?: never })
  | (BetterSqlite3StateClientOptions & { path?: never; databaseOptions?: never });

interface CacheRow {
  value: string;
  expires_at: number | null;
}

interface QueueRow {
  seq: number;
  value: string;
}

interface CountRow {
  depth: number;
}

export const BETTER_SQLITE3_CHAT_STATE_SCHEMA_STATEMENTS = [
  `
  CREATE TABLE IF NOT EXISTS chat_state_subscriptions (
    key_prefix TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
    PRIMARY KEY (key_prefix, thread_id)
  )
  `,
  `
  CREATE TABLE IF NOT EXISTS chat_state_locks (
    key_prefix TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    token TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
    PRIMARY KEY (key_prefix, thread_id)
  )
  `,
  `
  CREATE INDEX IF NOT EXISTS chat_state_locks_expires_idx
  ON chat_state_locks (expires_at)
  `,
  `
  CREATE TABLE IF NOT EXISTS chat_state_cache (
    key_prefix TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    value TEXT NOT NULL,
    expires_at INTEGER,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
    PRIMARY KEY (key_prefix, cache_key)
  )
  `,
  `
  CREATE INDEX IF NOT EXISTS chat_state_cache_expires_idx
  ON chat_state_cache (expires_at)
  `,
  `
  CREATE TABLE IF NOT EXISTS chat_state_lists (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    key_prefix TEXT NOT NULL,
    list_key TEXT NOT NULL,
    value TEXT NOT NULL,
    expires_at INTEGER
  )
  `,
  `
  CREATE INDEX IF NOT EXISTS chat_state_lists_key_idx
  ON chat_state_lists (key_prefix, list_key, seq)
  `,
  `
  CREATE INDEX IF NOT EXISTS chat_state_lists_expires_idx
  ON chat_state_lists (expires_at)
  `,
  `
  CREATE TABLE IF NOT EXISTS chat_state_queues (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    key_prefix TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    value TEXT NOT NULL,
    expires_at INTEGER NOT NULL
  )
  `,
  `
  CREATE INDEX IF NOT EXISTS chat_state_queues_key_idx
  ON chat_state_queues (key_prefix, thread_id, seq)
  `,
  `
  CREATE INDEX IF NOT EXISTS chat_state_queues_expires_idx
  ON chat_state_queues (expires_at)
  `,
] as const;

export class BetterSqlite3StateAdapter implements StateAdapter {
  private database: BetterSqlite3Database;
  private readonly databaseOptions: BetterSqlite3Options | undefined;
  private readonly closeOnDisconnect: boolean;
  private readonly enableWal: boolean;
  private readonly keyPrefix: string;
  private readonly logger: Logger;
  private readonly ownsDatabase: boolean;
  private readonly path?: string;
  private connected = false;

  constructor(
    options: BetterSqlite3StateAdapterOptions | BetterSqlite3StateClientOptions
  ) {
    this.keyPrefix = options.keyPrefix ?? "chat-sdk";
    this.logger =
      options.logger ?? new ConsoleLogger("info").child("better-sqlite3");

    if ("database" in options) {
      this.database = options.database;
      this.ownsDatabase = false;
      this.closeOnDisconnect = options.closeOnDisconnect ?? false;
      this.enableWal = options.enableWal ?? false;
      return;
    }

    this.path = options.path;
    this.databaseOptions = options.databaseOptions;
    this.database = openDatabase(options.path, options.databaseOptions);
    this.ownsDatabase = true;
    this.closeOnDisconnect = options.closeOnDisconnect ?? true;
    this.enableWal = options.enableWal ?? true;
  }

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      if (!this.database.open) {
        if (!this.path) {
          throw new Error("Cannot reopen an external database");
        }
        this.database = openDatabase(this.path, this.databaseOptions);
      }

      if (this.enableWal && this.path !== ":memory:") {
        this.database.pragma("journal_mode = WAL");
      }
      this.database.pragma("foreign_keys = ON");
      this.ensureSchema();
      this.connected = true;
    } catch (error) {
      if (this.ownsDatabase && this.database.open) {
        this.database.close();
      }
      this.logger.error("better-sqlite3 connect failed", { error });
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    this.connected = false;

    if (this.closeOnDisconnect && this.database.open) {
      this.database.close();
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  async subscribe(threadId: string): Promise<void> {
    this.ensureConnected();
    this.db
      .prepare(
        `
        INSERT OR IGNORE INTO chat_state_subscriptions (
          key_prefix,
          thread_id
        )
        VALUES (?, ?)
        `
      )
      .run(this.keyPrefix, threadId);
  }

  async unsubscribe(threadId: string): Promise<void> {
    this.ensureConnected();
    this.db
      .prepare(
        `
        DELETE FROM chat_state_subscriptions
        WHERE key_prefix = ?
          AND thread_id = ?
        `
      )
      .run(this.keyPrefix, threadId);
  }

  async isSubscribed(threadId: string): Promise<boolean> {
    this.ensureConnected();
    const row = this.db
      .prepare(
        `
        SELECT 1
        FROM chat_state_subscriptions
        WHERE key_prefix = ?
          AND thread_id = ?
        LIMIT 1
        `
      )
      .get(this.keyPrefix, threadId);

    return row != null;
  }

  async acquireLock(threadId: string, ttlMs: number): Promise<Lock | null> {
    this.ensureConnected();
    this.dropExpiredLock(threadId);

    const lock: Lock = {
      expiresAt: Date.now() + ttlMs,
      threadId,
      token: `better-sqlite3_${crypto.randomUUID()}`,
    };

    const result = this.db
      .prepare(
        `
        INSERT OR IGNORE INTO chat_state_locks (
          key_prefix,
          thread_id,
          token,
          expires_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        `
      )
      .run(
        this.keyPrefix,
        lock.threadId,
        lock.token,
        lock.expiresAt,
        Date.now()
      );

    return result.changes === 1 ? lock : null;
  }

  async forceReleaseLock(threadId: string): Promise<void> {
    this.ensureConnected();
    this.db
      .prepare(
        `
        DELETE FROM chat_state_locks
        WHERE key_prefix = ?
          AND thread_id = ?
        `
      )
      .run(this.keyPrefix, threadId);
  }

  async releaseLock(lock: Lock): Promise<void> {
    this.ensureConnected();
    this.db
      .prepare(
        `
        DELETE FROM chat_state_locks
        WHERE key_prefix = ?
          AND thread_id = ?
          AND token = ?
        `
      )
      .run(this.keyPrefix, lock.threadId, lock.token);
  }

  async extendLock(lock: Lock, ttlMs: number): Promise<boolean> {
    this.ensureConnected();
    this.dropExpiredLock(lock.threadId);
    const now = Date.now();

    const result = this.db
      .prepare(
        `
        UPDATE chat_state_locks
        SET expires_at = ?,
            updated_at = ?
        WHERE key_prefix = ?
          AND thread_id = ?
          AND token = ?
          AND expires_at > ?
        `
      )
      .run(
        now + ttlMs,
        now,
        this.keyPrefix,
        lock.threadId,
        lock.token,
        now
      );

    return result.changes === 1;
  }

  async get<T = unknown>(key: string): Promise<T | null> {
    this.ensureConnected();

    const row = this.db
      .prepare(
        `
        SELECT value, expires_at
        FROM chat_state_cache
        WHERE key_prefix = ?
          AND cache_key = ?
        `
      )
      .get(this.keyPrefix, key) as CacheRow | undefined;

    if (!row) {
      return null;
    }

    if (row.expires_at !== null && row.expires_at <= Date.now()) {
      this.dropExpiredCacheValue(key);
      return null;
    }

    return parseStoredJson<T>(row.value, `cache value for key "${key}"`);
  }

  async set<T = unknown>(
    key: string,
    value: T,
    ttlMs?: number
  ): Promise<void> {
    this.ensureConnected();

    this.db
      .prepare(
        `
        INSERT INTO chat_state_cache (
          key_prefix,
          cache_key,
          value,
          expires_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(key_prefix, cache_key) DO UPDATE SET
          value = excluded.value,
          expires_at = excluded.expires_at,
          updated_at = excluded.updated_at
        `
      )
      .run(
        this.keyPrefix,
        key,
        JSON.stringify(value),
        ttlToExpiresAt(ttlMs),
        Date.now()
      );
  }

  async setIfNotExists(
    key: string,
    value: unknown,
    ttlMs?: number
  ): Promise<boolean> {
    this.ensureConnected();
    this.dropExpiredCacheValue(key);

    const result = this.db
      .prepare(
        `
        INSERT OR IGNORE INTO chat_state_cache (
          key_prefix,
          cache_key,
          value,
          expires_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        `
      )
      .run(
        this.keyPrefix,
        key,
        JSON.stringify(value),
        ttlToExpiresAt(ttlMs),
        Date.now()
      );

    return result.changes === 1;
  }

  async delete(key: string): Promise<void> {
    this.ensureConnected();
    this.db
      .prepare(
        `
        DELETE FROM chat_state_cache
        WHERE key_prefix = ?
          AND cache_key = ?
        `
      )
      .run(this.keyPrefix, key);
  }

  async appendToList(
    key: string,
    value: unknown,
    options: { maxLength?: number; ttlMs?: number } = {}
  ): Promise<void> {
    this.ensureConnected();

    const append = this.db.transaction(() => {
      this.dropExpiredListValues(key);

      this.db
        .prepare(
          `
          INSERT INTO chat_state_lists (
            key_prefix,
            list_key,
            value,
            expires_at
          )
          VALUES (?, ?, ?, ?)
          `
        )
        .run(
          this.keyPrefix,
          key,
          JSON.stringify(value),
          ttlToExpiresAt(options.ttlMs)
        );

      if (typeof options.ttlMs === "number" && options.ttlMs > 0) {
        this.db
          .prepare(
            `
            UPDATE chat_state_lists
            SET expires_at = ?
            WHERE key_prefix = ?
              AND list_key = ?
            `
          )
          .run(Date.now() + options.ttlMs, this.keyPrefix, key);
      }

      if (typeof options.maxLength === "number") {
        if (options.maxLength <= 0) {
          this.clearList(key);
          return;
        }

        this.db
          .prepare(
            `
            DELETE FROM chat_state_lists
            WHERE key_prefix = ?
              AND list_key = ?
              AND seq NOT IN (
                SELECT seq
                FROM chat_state_lists
                WHERE key_prefix = ?
                  AND list_key = ?
                ORDER BY seq DESC
                LIMIT ?
              )
            `
          )
          .run(this.keyPrefix, key, this.keyPrefix, key, options.maxLength);
      }
    });

    append();
  }

  async getList<T = unknown>(key: string): Promise<T[]> {
    this.ensureConnected();
    this.dropExpiredListValues(key);

    const rows = this.db
      .prepare(
        `
        SELECT value
        FROM chat_state_lists
        WHERE key_prefix = ?
          AND list_key = ?
        ORDER BY seq ASC
        `
      )
      .all(this.keyPrefix, key) as Array<{ value: string }>;

    return rows.map((row) =>
      parseStoredJson<T>(row.value, `list entry for key "${key}"`)
    );
  }

  async enqueue(
    threadId: string,
    entry: QueueEntry,
    maxSize: number
  ): Promise<number> {
    this.ensureConnected();

    if (maxSize <= 0) {
      this.clearQueue(threadId);
      return 0;
    }

    const enqueue = this.db.transaction(() => {
      this.dropExpiredQueueEntries(threadId);

      this.db
        .prepare(
          `
          INSERT INTO chat_state_queues (
            key_prefix,
            thread_id,
            value,
            expires_at
          )
          VALUES (?, ?, ?, ?)
          `
        )
        .run(this.keyPrefix, threadId, JSON.stringify(entry), entry.expiresAt);

      this.db
        .prepare(
          `
          DELETE FROM chat_state_queues
          WHERE key_prefix = ?
            AND thread_id = ?
            AND seq NOT IN (
              SELECT seq
              FROM chat_state_queues
              WHERE key_prefix = ?
                AND thread_id = ?
              ORDER BY seq DESC
              LIMIT ?
            )
          `
        )
        .run(this.keyPrefix, threadId, this.keyPrefix, threadId, maxSize);

      return this.queueDepthSync(threadId);
    });

    return enqueue();
  }

  async dequeue(threadId: string): Promise<QueueEntry | null> {
    this.ensureConnected();

    const dequeue = this.db.transaction(() => {
      this.dropExpiredQueueEntries(threadId);

      const row = this.db
        .prepare(
          `
          SELECT seq, value
          FROM chat_state_queues
          WHERE key_prefix = ?
            AND thread_id = ?
          ORDER BY seq ASC
          LIMIT 1
          `
        )
        .get(this.keyPrefix, threadId) as QueueRow | undefined;

      if (!row) {
        return null;
      }

      this.db
        .prepare(
          `
          DELETE FROM chat_state_queues
          WHERE key_prefix = ?
            AND thread_id = ?
            AND seq = ?
          `
        )
        .run(this.keyPrefix, threadId, row.seq);

      return parseQueueEntry(row.value, threadId);
    });

    return dequeue();
  }

  async queueDepth(threadId: string): Promise<number> {
    this.ensureConnected();
    this.dropExpiredQueueEntries(threadId);
    return this.queueDepthSync(threadId);
  }

  getClient(): BetterSqlite3Database {
    return this.db;
  }

  migrate(): void {
    this.ensureSchema();
  }

  private get db(): BetterSqlite3Database {
    if (!this.database.open) {
      throw new Error(
        "BetterSqlite3StateAdapter database is closed. Call connect() first."
      );
    }
    return this.database;
  }

  private ensureSchema(): void {
    for (const statement of BETTER_SQLITE3_CHAT_STATE_SCHEMA_STATEMENTS) {
      this.db.prepare(statement).run();
    }
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new Error(
        "BetterSqlite3StateAdapter is not connected. Call connect() first."
      );
    }
  }

  private queueDepthSync(threadId: string): number {
    const row = this.db
      .prepare(
        `
        SELECT COUNT(*) AS depth
        FROM chat_state_queues
        WHERE key_prefix = ?
          AND thread_id = ?
          AND expires_at > ?
        `
      )
      .get(this.keyPrefix, threadId, Date.now()) as CountRow | undefined;

    return row?.depth ?? 0;
  }

  private clearList(key: string): void {
    this.db
      .prepare(
        `
        DELETE FROM chat_state_lists
        WHERE key_prefix = ?
          AND list_key = ?
        `
      )
      .run(this.keyPrefix, key);
  }

  private clearQueue(threadId: string): void {
    this.db
      .prepare(
        `
        DELETE FROM chat_state_queues
        WHERE key_prefix = ?
          AND thread_id = ?
        `
      )
      .run(this.keyPrefix, threadId);
  }

  private dropExpiredCacheValue(key: string): void {
    this.db
      .prepare(
        `
        DELETE FROM chat_state_cache
        WHERE key_prefix = ?
          AND cache_key = ?
          AND expires_at IS NOT NULL
          AND expires_at <= ?
        `
      )
      .run(this.keyPrefix, key, Date.now());
  }

  private dropExpiredListValues(key: string): void {
    this.db
      .prepare(
        `
        DELETE FROM chat_state_lists
        WHERE key_prefix = ?
          AND list_key = ?
          AND expires_at IS NOT NULL
          AND expires_at <= ?
        `
      )
      .run(this.keyPrefix, key, Date.now());
  }

  private dropExpiredLock(threadId: string): void {
    this.db
      .prepare(
        `
        DELETE FROM chat_state_locks
        WHERE key_prefix = ?
          AND thread_id = ?
          AND expires_at <= ?
        `
      )
      .run(this.keyPrefix, threadId, Date.now());
  }

  private dropExpiredQueueEntries(threadId: string): void {
    this.db
      .prepare(
        `
        DELETE FROM chat_state_queues
        WHERE key_prefix = ?
          AND thread_id = ?
          AND expires_at <= ?
        `
      )
      .run(this.keyPrefix, threadId, Date.now());
  }
}

export function createBetterSqlite3State(
  options: CreateBetterSqlite3StateOptions = {}
): BetterSqlite3StateAdapter {
  if (
    "database" in options &&
    options.database &&
    (("path" in options && options.path) ||
      ("databaseOptions" in options && options.databaseOptions))
  ) {
    throw new Error(
      "Provide either database or path/databaseOptions, not both."
    );
  }

  if ("database" in options && options.database) {
    return new BetterSqlite3StateAdapter({
      closeOnDisconnect: options.closeOnDisconnect,
      database: options.database,
      enableWal: options.enableWal,
      keyPrefix: options.keyPrefix,
      logger: options.logger,
    });
  }

  return new BetterSqlite3StateAdapter({
    closeOnDisconnect: options.closeOnDisconnect,
    databaseOptions: options.databaseOptions,
    enableWal: options.enableWal,
    keyPrefix: options.keyPrefix,
    logger: options.logger,
    path:
      options.path ??
      process.env.CHAT_STATE_SQLITE_PATH ??
      process.env.SQLITE_PATH ??
      "./chat-state.sqlite",
  });
}

function openDatabase(
  path: string,
  options?: BetterSqlite3Options
): BetterSqlite3Database {
  if (path.startsWith("file:")) {
    throw new Error(
      "SQLite URI filenames are not supported. Provide a filesystem path instead."
    );
  }

  if (path !== ":memory:") {
    mkdirSync(dirname(path), { recursive: true });
  }

  return new DatabaseConstructor(path, options);
}

function ttlToExpiresAt(ttlMs: number | undefined): number | null {
  return typeof ttlMs === "number" && ttlMs > 0 ? Date.now() + ttlMs : null;
}

function parseStoredJson<T>(raw: string, label: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch (error) {
    throw new Error(
      `BetterSqlite3StateAdapter expected JSON-encoded ${label}`,
      { cause: error }
    );
  }
}

function parseQueueEntry(raw: string, threadId: string): QueueEntry {
  const entry = parseStoredJson<QueueEntry>(
    raw,
    `queue entry for thread "${threadId}"`
  );
  const serialized = serializedMessage(entry.message);

  if (serialized) {
    entry.message = Message.fromJSON(serialized);
  }

  return entry;
}

function serializedMessage(
  value: unknown
): Parameters<typeof Message.fromJSON>[0] | null {
  if (
    typeof value === "object" &&
    value !== null &&
    "_type" in value &&
    value._type === "chat:Message"
  ) {
    return value as Parameters<typeof Message.fromJSON>[0];
  }

  return null;
}
