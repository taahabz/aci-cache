import { describe, it, expect, vi } from "vitest";
import { Controller } from "../src/controller";

describe("Controller.selectStrategy", () => {
  const cases: [number, string][] = [
    [0, "ttl"],
    [5, "ttl"],
    [9, "ttl"],
    [9.99, "ttl"],
    [10, "batched"],
    [11, "batched"],
    [25, "batched"],
    [49, "batched"],
    [50, "batched"],
    [50.01, "eager"],
    [51, "eager"],
    [100, "eager"],
    [1000, "eager"],
  ];

  it.each(cases)(
    "should select %s for write rate %d",
    (writeRate, expected) => {
      const result = Controller.selectStrategy(writeRate, 50, 10);
      expect(result).toBe(expected);
    }
  );

  it("should work with custom thresholds", () => {
    expect(Controller.selectStrategy(5, 20, 3)).toBe("batched");
    expect(Controller.selectStrategy(2, 20, 3)).toBe("ttl");
    expect(Controller.selectStrategy(25, 20, 3)).toBe("eager");
  });
});
