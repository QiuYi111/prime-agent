import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getModel } from "../src/models.js";
import { streamSimple } from "../src/stream.js";
import type { AssistantMessage, AssistantMessageEvent, Context, ThinkingContent } from "../src/types.js";
import {
	LOCAL_THINKING_LEVEL_LIMITS,
	ReasoningRunawayGuard,
	resolveReasoningLimits,
} from "../src/utils/reasoning-limits.js";

const mockState = vi.hoisted(() => ({
	lastParams: undefined as any,
	thinkingChunk: "t".repeat(500),
	/** Emit a usage chunk once this many reasoning chunks were sent. */
	usageAfterChunks: Number.POSITIVE_INFINITY,
	usage: undefined as any,
	/** Finish normally after this many chunks (Infinity = runaway). */
	maxChunks: Number.POSITIVE_INFINITY,
	delayMs: 0,
	sawAbort: false,
}));

vi.mock("openai", () => {
	class FakeOpenAI {
		chat = {
			completions: {
				create: (params: unknown, requestOptions: { signal?: AbortSignal } | undefined) => {
					mockState.lastParams = params;
					const signal = requestOptions?.signal;
					const stream = {
						async *[Symbol.asyncIterator]() {
							let chunkIndex = 0;
							while (true) {
								if (signal?.aborted) {
									mockState.sawAbort = true;
									const error = new Error("Request was aborted.");
									error.name = "AbortError";
									throw error;
								}
								if (mockState.delayMs > 0) {
									await new Promise((resolve) => setTimeout(resolve, mockState.delayMs));
								}
								if (chunkIndex >= mockState.maxChunks) {
									yield {
										id: "chunk-final",
										model: "glm-5.3",
										choices: [{ delta: { content: "done" }, finish_reason: "stop" }],
									};
									return;
								}
								yield {
									id: "chunk-thinking",
									model: "glm-5.3",
									choices: [{ delta: { reasoning_content: mockState.thinkingChunk }, finish_reason: null }],
								};
								chunkIndex++;
								if (chunkIndex >= mockState.usageAfterChunks && mockState.usage) {
									yield { id: "chunk-usage", model: "glm-5.3", choices: [], usage: mockState.usage };
									mockState.usageAfterChunks = Number.POSITIVE_INFINITY;
								}
							}
						},
					};
					const promise = Promise.resolve(stream) as Promise<typeof stream> & {
						withResponse: () => Promise<{
							data: typeof stream;
							response: { status: number; headers: Headers };
						}>;
					};
					promise.withResponse = async () => ({
						data: stream,
						response: { status: 200, headers: new Headers() },
					});
					return promise;
				},
			},
		};
	}

	return { default: FakeOpenAI };
});

const context: Context = {
	messages: [{ role: "user", content: "Design a mounting plate.", timestamp: Date.now() }],
};

async function collect(
	streamResult: ReturnType<typeof streamSimple>,
): Promise<{ events: AssistantMessageEvent[]; message: AssistantMessage }> {
	const events: AssistantMessageEvent[] = [];
	for await (const event of streamResult) {
		events.push(event);
	}
	return { events, message: await streamResult.result() };
}

function thinkingOf(message: AssistantMessage): string {
	return message.content
		.filter((block): block is ThinkingContent => block.type === "thinking")
		.map((block) => block.thinking)
		.join("");
}

function zaiModel() {
	const model = getModel("zai", "glm-5.3")!;
	expect(model.compat?.thinkingFormat).toBe("zai");
	expect(model.reasoning).toBe(true);
	return model;
}

beforeEach(() => {
	mockState.lastParams = undefined;
	mockState.usage = undefined;
	mockState.usageAfterChunks = Number.POSITIVE_INFINITY;
	mockState.maxChunks = Number.POSITIVE_INFINITY;
	mockState.delayMs = 0;
	mockState.sawAbort = false;
	mockState.thinkingChunk = "t".repeat(500);
});

afterEach(() => {
	vi.restoreAllMocks();
});

describe("reasoning runaway guard", () => {
	it("terminates a thinking-only stream inside the configured budget", async () => {
		const model = zaiModel();
		const { events, message } = await collect(streamSimple(model, context, { apiKey: "test", reasoning: "medium" }));

		const limit = LOCAL_THINKING_LEVEL_LIMITS.medium.maxThinkingChars!;
		const deliveredThinking = events
			.filter((event) => event.type === "thinking_delta")
			.map((event) => (event.type === "thinking_delta" ? event.delta.length : 0))
			.reduce((total, length) => total + length, 0);
		expect(message.stopReason).toBe("reasoning_limit");
		expect(message.reasoningLimit).toEqual({
			reason: "max_thinking_chars",
			limit,
			observed: limit,
		});
		expect(message.errorMessage).toContain("Reasoning limit reached");
		// Nothing beyond the budget reaches the caller.
		expect(deliveredThinking).toBe(limit);
		// The partial message survives: no text, no tool call, just the thinking that arrived.
		expect(message.content.every((block) => block.type === "thinking")).toBe(true);
		expect(thinkingOf(message).length).toBeGreaterThanOrEqual(limit);
		expect(mockState.sawAbort).toBe(true);
	});

	it("reports usage as unavailable instead of a real zero after the limit", async () => {
		const model = zaiModel();
		const { message } = await collect(streamSimple(model, context, { apiKey: "test", reasoning: "minimal" }));

		expect(message.stopReason).toBe("reasoning_limit");
		expect(message.usage.totalTokens).toBe(0);
		expect(message.usageUnavailable).toBe("reasoning_limit");
	});

	it("keeps usage the provider already reported before the limit", async () => {
		const model = zaiModel();
		mockState.usageAfterChunks = 1;
		mockState.usage = {
			prompt_tokens: 120,
			completion_tokens: 80,
			total_tokens: 200,
			prompt_tokens_details: { cached_tokens: 0 },
		};

		const { message } = await collect(
			streamSimple(model, context, {
				apiKey: "test",
				reasoning: "medium",
				reasoningLimits: { maxThinkingChars: 2_000 },
			}),
		);

		expect(message.stopReason).toBe("reasoning_limit");
		expect(message.usage.input).toBe(120);
		expect(message.usage.output).toBe(80);
		expect(message.usageUnavailable).toBeUndefined();
	});

	it("separates a user abort from a reasoning limit", async () => {
		const model = zaiModel();
		const controller = new AbortController();
		const streamResult = streamSimple(model, context, {
			apiKey: "test",
			reasoning: "medium",
			signal: controller.signal,
		});

		let thinkingDeltas = 0;
		for await (const event of streamResult) {
			if (event.type === "thinking_delta" && ++thinkingDeltas === 3) {
				controller.abort();
			}
		}
		const message = await streamResult.result();

		expect(message.stopReason).toBe("aborted");
		expect(message.reasoningLimit).toBeUndefined();
		expect(thinkingOf(message).length).toBeGreaterThan(0);
		// The provider never reached its final usage chunk, so the zeros are not measurements.
		expect(message.usageUnavailable).toBe("aborted");
	});

	it("stops a thinking phase that runs too long even when no character limit trips", async () => {
		const model = zaiModel();
		mockState.thinkingChunk = "t".repeat(4);
		mockState.delayMs = 15;

		const { message } = await collect(
			streamSimple(model, context, {
				apiKey: "test",
				reasoning: "medium",
				reasoningLimits: { maxThinkingMs: 40 },
			}),
		);

		expect(message.stopReason).toBe("reasoning_limit");
		expect(message.reasoningLimit?.reason).toBe("max_thinking_ms");
	});

	it("does not trip while the model interleaves thinking with output", async () => {
		const guard = new ReasoningRunawayGuard({ maxThinkingChars: 1_000 });

		expect(guard.observeThinking(900)).toBeUndefined();
		guard.reset();
		expect(guard.observeThinking(900)).toBeUndefined();
		expect(guard.observeThinking(200)).toEqual({
			reason: "max_thinking_chars",
			limit: 1_000,
			observed: 1_100,
		});
	});

	it("can be disabled per request", async () => {
		const model = zaiModel();
		mockState.maxChunks = 3;

		const { message } = await collect(
			streamSimple(model, context, { apiKey: "test", reasoning: "medium", reasoningLimits: false }),
		);

		expect(message.stopReason).toBe("stop");
		expect(message.reasoningLimit).toBeUndefined();
		expect(thinkingOf(message).length).toBe(1_500);
		expect(message.content.some((block) => block.type === "text")).toBe(true);
	});
});

describe("binary thinking capability", () => {
	it("gives each thinking level a real local budget", () => {
		const model = zaiModel();
		const minimal = resolveReasoningLimits(model, { reasoning: "minimal" })!;
		const medium = resolveReasoningLimits(model, { reasoning: "medium" })!;
		const high = resolveReasoningLimits(model, { reasoning: "high" })!;

		expect(minimal.source).toBe("local");
		expect(medium.level).toBe("medium");
		expect(minimal.limits.maxThinkingChars!).toBeLessThan(medium.limits.maxThinkingChars!);
		expect(medium.limits.maxThinkingChars!).toBeLessThan(high.limits.maxThinkingChars!);
		// Medium must not be able to spend a quarter of an hour thinking.
		expect(medium.limits.maxThinkingMs!).toBeLessThanOrEqual(300_000);
	});

	it("does not claim a local budget where the provider has a real one", () => {
		const model = getModel("openai", "gpt-5.6")!;
		expect(resolveReasoningLimits(model, { reasoning: "medium" })).toBeUndefined();
	});

	it("skips the guard when thinking is explicitly off", () => {
		const model = zaiModel();
		expect(resolveReasoningLimits(model, { reasoning: "off" })).toBeUndefined();
	});

	it("sends no fake graded effort for a binary provider", async () => {
		const model = zaiModel();
		mockState.maxChunks = 1;

		await collect(streamSimple(model, context, { apiKey: "test", reasoning: "minimal" }));
		const minimalParams = mockState.lastParams;
		await collect(streamSimple(model, context, { apiKey: "test", reasoning: "high" }));
		const highParams = mockState.lastParams;

		for (const params of [minimalParams, highParams]) {
			expect(params.enable_thinking).toBe(true);
			// z.ai ignores reasoning_effort here; the levels live in the local guard.
			expect(params.reasoning_effort).toBeUndefined();
		}
	});
});
