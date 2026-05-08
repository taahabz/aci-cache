import { describe, it, expect, vi } from "vitest";
import { TTLStrategy } from "../src/strategies/ttl";
import { EagerStrategy } from "../src/strategies/eager";
import { BatchedStrategy } from "../src/strategies/batched";

// Mock ioredis client
function createMockRedis() {
  return {
    publish: vi.fn().mockResolvedValue(1),
  } as any;
}

describe("TTLStrategy", () => {
  it("should have name 'ttl'", () => {
    expect(new TTLStrategy().name).toBe("ttl");
  });

  it("onWrite should be a no-op", async () => {
    const s = new TTLStrategy();
    await expect(s.onWrite("key")).resolves.toBeUndefined();
  });
});

describe("EagerStrategy", () => {
  it("should have name 'eager'", () => {
    const mock = createMockRedis();
    expect(new EagerStrategy(mock, "ch", "inst").name).toBe("eager");
  });

  it("should publish on every write", async () => {
    const mock = createMockRedis();
    const s = new EagerStrategy(mock, "aci:invalidation", "test_inst");
    await s.onWrite("user:123");

    expect(mock.publish).toHaveBeenCalledOnce();
    const [channel, raw] = mock.publish.mock.calls[0];
    expect(channel).toBe("aci:invalidation");

    const payload = JSON.parse(raw);
    expect(payload.action).toBe("invalidate");
    expect(payload.keys).toEqual(["user:123"]);
    expect(payload.strategy).toBe("eager");
    expect(payload.source).toBe("test_inst");
    expect(payload.timestamp).toBeTypeOf("number");
  });

  it("should swallow connection errors", async () => {
    const mock = createMockRedis();
    mock.publish.mockRejectedValue(new Error("gone"));
    const s = new EagerStrategy(mock, "ch", "test");
    await expect(s.onWrite("key")).resolves.toBeUndefined();
  });
});

describe("BatchedStrategy", () => {
  it("should have name 'batched'", () => {
    const mock = createMockRedis();
    expect(new BatchedStrategy(mock, "ch", "inst").name).toBe("batched");
  });

  it("should buffer keys without publishing", async () => {
    const mock = createMockRedis();
    const s = new BatchedStrategy(mock, "ch", "inst");
    await s.onWrite("key1");
    await s.onWrite("key2");
    expect(mock.publish).not.toHaveBeenCalled();
  });

  it("should publish buffered keys on flush", async () => {
    const mock = createMockRedis();
    const s = new BatchedStrategy(mock, "aci:invalidation", "test_inst");
    await s.onWrite("key1");
    await s.onWrite("key2");
    await s.flush();

    expect(mock.publish).toHaveBeenCalledOnce();
    const payload = JSON.parse(mock.publish.mock.calls[0][1]);
    expect(new Set(payload.keys)).toEqual(new Set(["key1", "key2"]));
    expect(payload.strategy).toBe("batched");
  });

  it("should deduplicate keys", async () => {
    const mock = createMockRedis();
    const s = new BatchedStrategy(mock, "ch", "inst");
    await s.onWrite("key1");
    await s.onWrite("key1");
    await s.onWrite("key1");
    await s.flush();

    const payload = JSON.parse(mock.publish.mock.calls[0][1]);
    expect(payload.keys).toEqual(["key1"]);
  });

  it("should no-op on flush with empty buffer", async () => {
    const mock = createMockRedis();
    const s = new BatchedStrategy(mock, "ch", "inst");
    await s.flush();
    expect(mock.publish).not.toHaveBeenCalled();
  });

  it("should clear buffer after flush", async () => {
    const mock = createMockRedis();
    const s = new BatchedStrategy(mock, "ch", "inst");
    await s.onWrite("key1");
    await s.flush();
    mock.publish.mockClear();
    await s.flush();
    expect(mock.publish).not.toHaveBeenCalled();
  });

  it("should flush on deactivate", async () => {
    const mock = createMockRedis();
    const s = new BatchedStrategy(mock, "ch", "inst");
    await s.onWrite("leftover");
    await s.onDeactivate();

    expect(mock.publish).toHaveBeenCalledOnce();
    const payload = JSON.parse(mock.publish.mock.calls[0][1]);
    expect(payload.keys).toEqual(["leftover"]);
  });
});
