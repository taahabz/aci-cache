import { describe, it, expect } from "vitest";
import { WriteRateTracker } from "../src/tracker";

describe("WriteRateTracker", () => {
  describe("basic rate", () => {
    it("should return 0 for empty tracker", () => {
      const tracker = new WriteRateTracker(5);
      expect(tracker.getRate()).toBe(0);
    });

    it("should calculate rate for single write", () => {
      const tracker = new WriteRateTracker(5);
      tracker.record();
      expect(tracker.getRate()).toBeCloseTo(0.2, 1);
    });

    it("should calculate rate for multiple writes", () => {
      const tracker = new WriteRateTracker(5);
      for (let i = 0; i < 10; i++) tracker.record();
      expect(tracker.getRate()).toBeCloseTo(2.0, 1);
    });

    it("should return correct count", () => {
      const tracker = new WriteRateTracker(5);
      for (let i = 0; i < 5; i++) tracker.record();
      expect(tracker.getCount()).toBe(5);
    });
  });

  describe("window expiry", () => {
    it("should trim expired writes", async () => {
      const tracker = new WriteRateTracker(0.1); // 100ms window
      tracker.record();
      await new Promise((r) => setTimeout(r, 150));
      expect(tracker.getRate()).toBe(0);
      expect(tracker.getCount()).toBe(0);
    });
  });

  describe("validation", () => {
    it("should throw on zero window", () => {
      expect(() => new WriteRateTracker(0)).toThrow("positive");
    });

    it("should throw on negative window", () => {
      expect(() => new WriteRateTracker(-1)).toThrow("positive");
    });
  });
});
