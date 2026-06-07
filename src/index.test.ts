import Database from "better-sqlite3";
import { Chat, Message, paragraph, root, text } from "chat";
import { createMockAdapter, createTestMessage } from "@chat-adapter/tests";
import type { Lock, MessageData } from "chat";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BetterSqlite3StateAdapter,
  createBetterSqlite3State,
} from "./index";

const tempDirs: string[] = [];

afterEach(() => {
  vi.unstubAllEnvs();

  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { force: true, recursive: true });
  }
});

function tempPath(file = "chat-state.sqlite"): string {
  const dir = mkdtempSync(join(tmpdir(), "chat-state-better-sqlite3-"));
  tempDirs.push(dir);
  return join(dir, file);
}

async function connectedState(path = ":memory:"): Promise<BetterSqlite3StateAdapter> {
  const state = createBetterSqlite3State({ enableWal: false, path });
  await state.connect();
  return state;
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function message(value: string): Message {
  return new Message({
    attachments: [],
    author: {
      fullName: "User One",
      isBot: false,
      isMe: false,
      userId: "user-1",
      userName: "user-one",
    },
    formatted: root([paragraph([text(value)])]),
    id: `message-${value}`,
    isMention: false,
    links: [],
    metadata: {
      dateSent: new Date("2026-06-06T00:00:00.000Z"),
      edited: false,
    },
    raw: {},
    text: value,
    threadId: "thread",
  } satisfies MessageData);
}

describe("BetterSqlite3StateAdapter", () => {
  it("exports a factory and adapter class", () => {
    expect(typeof createBetterSqlite3State).toBe("function");
    expect(typeof BetterSqlite3StateAdapter).toBe("function");
  });

  it("creates state from an explicit path, env path, or existing database", () => {
    expect(createBetterSqlite3State({ path: ":memory:" })).toBeInstanceOf(
      BetterSqlite3StateAdapter
    );

    vi.stubEnv("CHAT_STATE_SQLITE_PATH", ":memory:");
    expect(createBetterSqlite3State()).toBeInstanceOf(
      BetterSqlite3StateAdapter
    );

    const database = new Database(":memory:");
    const state = createBetterSqlite3State({ database });
    expect(state).toBeInstanceOf(BetterSqlite3StateAdapter);
    expect(() =>
      createBetterSqlite3State({ database, path: ":memory:" } as never)
    ).toThrow("Provide either database or path/databaseOptions");
    database.close();
  });

  it("rejects SQLite URI filenames instead of creating literal file: paths", () => {
    expect(() =>
      createBetterSqlite3State({ path: "file::memory:?cache=shared" })
    ).toThrow("SQLite URI filenames are not supported");
  });

  it("throws when methods are used before connect", async () => {
    const state = createBetterSqlite3State({ path: ":memory:" });
    const lock: Lock = {
      expiresAt: Date.now() + 1000,
      threadId: "thread",
      token: "token",
    };

    await expect(state.subscribe("thread")).rejects.toThrow("not connected");
    await expect(state.unsubscribe("thread")).rejects.toThrow("not connected");
    await expect(state.isSubscribed("thread")).rejects.toThrow("not connected");
    await expect(state.acquireLock("thread", 1000)).rejects.toThrow(
      "not connected"
    );
    await expect(state.releaseLock(lock)).rejects.toThrow("not connected");
    await expect(state.extendLock(lock, 1000)).rejects.toThrow("not connected");
    await expect(state.get("key")).rejects.toThrow("not connected");
    await expect(state.set("key", "value")).rejects.toThrow("not connected");
    await expect(state.setIfNotExists("key", "value")).rejects.toThrow(
      "not connected"
    );
    await expect(state.delete("key")).rejects.toThrow("not connected");
    await expect(state.appendToList("key", "value")).rejects.toThrow(
      "not connected"
    );
    await expect(state.getList("key")).rejects.toThrow("not connected");
    await expect(
      state.enqueue(
        "thread",
        {
          enqueuedAt: Date.now(),
          expiresAt: Date.now() + 1000,
          message: message("hi"),
        },
        10
      )
    ).rejects.toThrow("not connected");
    await expect(state.dequeue("thread")).rejects.toThrow("not connected");
    await expect(state.queueDepth("thread")).rejects.toThrow("not connected");
  });

  it("is idempotent across connect and disconnect", async () => {
    const state = createBetterSqlite3State({ enableWal: false, path: tempPath() });

    await state.connect();
    await state.connect();
    expect(state.isConnected()).toBe(true);

    await state.disconnect();
    await state.disconnect();
    expect(state.isConnected()).toBe(false);

    await state.connect();
    expect(state.isConnected()).toBe(true);
    await state.disconnect();
  });

  it("leaves an external database open by default", async () => {
    const database = new Database(":memory:");
    const state = createBetterSqlite3State({ database });

    await state.connect();
    await state.disconnect();

    expect(database.open).toBe(true);
    database.close();
  });

  it("subscribes and unsubscribes by thread id", async () => {
    const state = await connectedState();

    expect(await state.isSubscribed("thread")).toBe(false);
    await state.subscribe("thread");
    await state.subscribe("thread");
    expect(await state.isSubscribed("thread")).toBe(true);
    await state.unsubscribe("thread");
    expect(await state.isSubscribed("thread")).toBe(false);
  });

  it("stores cache values with ttl and set-if-not-exists semantics", async () => {
    const state = await connectedState();

    await state.set("answer", { value: 42 });
    expect(await state.get("answer")).toEqual({ value: 42 });

    expect(await state.setIfNotExists("answer", "later")).toBe(false);
    expect(await state.get("answer")).toEqual({ value: 42 });

    await state.set("short", "lived", 50);
    expect(await state.get("short")).toBe("lived");
    await wait(80);
    expect(await state.get("short")).toBeNull();

    expect(await state.setIfNotExists("short", "again")).toBe(true);
    expect(await state.get("short")).toBe("again");

    await state.set("durable", "zero", 0);
    expect(await state.get("durable")).toBe("zero");

    await state.delete("answer");
    expect(await state.get("answer")).toBeNull();
  });

  it("namespaces cache, subscriptions, and locks by key prefix", async () => {
    const database = new Database(":memory:");
    const first = createBetterSqlite3State({
      database,
      keyPrefix: "first",
    });
    const second = createBetterSqlite3State({
      database,
      keyPrefix: "second",
    });

    await first.connect();
    await second.connect();

    await first.set("shared", "first");
    await first.subscribe("thread");
    const firstLock = await first.acquireLock("thread", 1000);

    expect(await second.get("shared")).toBeNull();
    expect(await second.isSubscribed("thread")).toBe(false);
    expect(await second.acquireLock("thread", 1000)).not.toBeNull();
    expect(firstLock).not.toBeNull();

    database.close();
  });

  it("acquires, extends, releases, and force-releases locks", async () => {
    const state = await connectedState();

    const lock = await state.acquireLock("thread", 1000);
    expect(lock).not.toBeNull();
    expect(await state.acquireLock("thread", 1000)).toBeNull();

    expect(await state.extendLock(lock!, 1000)).toBe(true);
    await state.releaseLock(lock!);

    const nextLock = await state.acquireLock("thread", 1000);
    expect(nextLock).not.toBeNull();
    await state.forceReleaseLock("thread");
    expect(await state.acquireLock("thread", 1000)).not.toBeNull();
  });

  it("replaces expired locks", async () => {
    const state = await connectedState();

    const lock = await state.acquireLock("thread", 50);
    expect(lock).not.toBeNull();
    await wait(80);

    expect(await state.extendLock(lock!, 1000)).toBe(false);

    const nextLock = await state.acquireLock("thread", 1000);
    expect(nextLock).not.toBeNull();
    expect(nextLock?.token).not.toBe(lock?.token);
  });

  it("appends, trims, expires, and reads lists in insertion order", async () => {
    const state = await connectedState();

    await state.appendToList("history", "one", { maxLength: 2 });
    await state.appendToList("history", "two", { maxLength: 2 });
    await state.appendToList("history", "three", { maxLength: 2 });
    expect(await state.getList("history")).toEqual(["two", "three"]);

    await state.appendToList("durable", "zero", { ttlMs: 0 });
    expect(await state.getList("durable")).toEqual(["zero"]);

    await state.appendToList("ephemeral", "one", { ttlMs: 50 });
    await state.appendToList("ephemeral", "two", { ttlMs: 100 });
    expect(await state.getList("ephemeral")).toEqual(["one", "two"]);
    await wait(140);
    expect(await state.getList("ephemeral")).toEqual([]);
  });

  it("queues entries in FIFO order, trims to max size, and rehydrates messages", async () => {
    const state = await connectedState();
    const now = Date.now();

    await state.enqueue(
      "thread",
      { enqueuedAt: now, expiresAt: now + 1000, message: message("one") },
      2
    );
    await state.enqueue(
      "thread",
      { enqueuedAt: now + 1, expiresAt: now + 1000, message: message("two") },
      2
    );
    const depth = await state.enqueue(
      "thread",
      { enqueuedAt: now + 2, expiresAt: now + 1000, message: message("three") },
      2
    );

    expect(depth).toBe(2);
    expect(await state.queueDepth("thread")).toBe(2);

    const first = await state.dequeue("thread");
    expect(first?.message).toBeInstanceOf(Message);
    expect(first?.message.text).toBe("two");

    const second = await state.dequeue("thread");
    expect(second?.message.text).toBe("three");
    expect(await state.dequeue("thread")).toBeNull();
  });

  it("drops expired queue entries and clears when max size is zero", async () => {
    const state = await connectedState();
    const now = Date.now();

    await state.enqueue(
      "thread",
      { enqueuedAt: now, expiresAt: now + 50, message: message("expired") },
      10
    );
    await wait(80);
    expect(await state.queueDepth("thread")).toBe(0);
    expect(await state.dequeue("thread")).toBeNull();

    await state.enqueue(
      "thread",
      {
        enqueuedAt: now,
        expiresAt: Date.now() + 1000,
        message: message("gone"),
      },
      0
    );
    expect(await state.queueDepth("thread")).toBe(0);
  });

  it("uses the public WAL default for owned file databases", async () => {
    const path = tempPath();
    const first = createBetterSqlite3State({ path });

    await first.connect();
    expect(first.getClient().pragma("journal_mode", { simple: true })).toBe(
      "wal"
    );
    await first.set("key", "value");
    await first.disconnect();

    const second = createBetterSqlite3State({ path });
    await second.connect();
    expect(await second.get("key")).toBe("value");
    await second.disconnect();
  });

  it("persists state across owned database reconnects", async () => {
    const path = tempPath();
    const first = createBetterSqlite3State({ enableWal: false, path });

    await first.connect();
    await first.set("key", "value");
    await first.subscribe("thread");
    await first.disconnect();

    const second = createBetterSqlite3State({ enableWal: false, path });
    await second.connect();

    expect(await second.get("key")).toBe("value");
    expect(await second.isSubscribed("thread")).toBe(true);
    await second.disconnect();
  });

  it("works as Chat SDK state in a mention and subscription flow", async () => {
    const state = await connectedState();
    const adapter = createMockAdapter("test", {
      postMessage: vi.fn().mockResolvedValue({
        id: "reply",
        raw: {},
        threadId: "test:C1:T1",
      }),
    });
    const chat = new Chat({
      adapters: { test: adapter },
      state,
      userName: "bot",
    });
    const events: string[] = [];
    const threadId = "test:C1:T1";

    chat.onNewMention(async (thread, incoming) => {
      events.push(`mention:${incoming.text}`);
      await thread.subscribe();
      await thread.post("subscribed");
    });

    chat.onSubscribedMessage(async (thread, incoming) => {
      events.push(`subscribed:${incoming.text}`);
      await thread.post("follow-up");
    });

    await chat.initialize();
    await chat.processMessage(
      adapter,
      threadId,
      createTestMessage("m1", "@bot hello", {
        isMention: true,
        threadId,
      })
    );
    await chat.processMessage(
      adapter,
      threadId,
      createTestMessage("m2", "still there?", {
        isMention: false,
        threadId,
      })
    );

    expect(events).toEqual(["mention:@bot hello", "subscribed:still there?"]);
    expect(await state.isSubscribed(threadId)).toBe(true);
    expect(adapter.postMessage).toHaveBeenCalledTimes(2);

    await chat.shutdown();
  });
});
