import { describe, expect, it } from "vitest";
import { summaryModelTarget } from "../src/modes/daemon/daemon-session-summarizer.js";

describe("status summarizer model selection", () => {
	it("stays unset when the deployment names no summary model", () => {
		expect(summaryModelTarget({})).toBeUndefined();
		expect(summaryModelTarget({ PRIME_SUMMARY_PROVIDER: "deepseek" })).toBeUndefined();
		expect(summaryModelTarget({ PRIME_SUMMARY_MODEL: "deepseek-flash" })).toBeUndefined();
		expect(summaryModelTarget({ PRIME_SUMMARY_PROVIDER: "  ", PRIME_SUMMARY_MODEL: "deepseek-flash" })).toBeUndefined();
	});

	it("uses only the explicitly configured provider and model", () => {
		expect(summaryModelTarget({ PRIME_SUMMARY_PROVIDER: "deepseek", PRIME_SUMMARY_MODEL: "deepseek-flash" })).toEqual({
			provider: "deepseek",
			model: "deepseek-flash",
		});
	});
});
