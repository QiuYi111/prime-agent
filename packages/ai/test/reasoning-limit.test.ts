import { getEventListeners } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerApiProvider, unregisterApiProviders } from "../src/api-registry.js";
import { getModel } from "../src/models.js";
import { stream, streamSimple } from "../src/stream.js";
import type {
	AssistantMessage,
	AssistantMessageEvent,
	AssistantMessageEventStream,
	Context,
	Model,
	StreamFunction,
	StreamOptions,
	TextContent,
	ThinkingContent,
	ToolCall,
} from "../src/types.js";
import { createAssistantMessageEventStream } from "../src/utils/event-stream.js";
import {
	LOCAL_THINKING_LEVEL_LIMITS,
	ReasoningRunawayGuard,
	resolveReasoningLimits,
} from "../src/utils/reasoning-limits.js";

const mockState = vi.hoisted(() => ({
	lastParams: undefined as any,
	lastSignal: undefined as AbortSignal | undefined,
	thinkingChunk: "t".repeat(500),
	/** Emit a usage chunk once this many reasoning chunks were sent. */
	usageAfterChunks: Number.POSITIVE_INFINITY,
	usage: undefined as any,
	/** Finish normally after this many chunks (Infinity = runaway). */
	maxChunks: Number.POSITIVE_INFINITY,
	/** Sit silent and ignore the abort once this many chunks were sent. */
	silentAfterChunks: Number.POSITIVE_INFINITY,
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
					mockState.lastSignal = signal;
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
								if (chunkIndex >= mockState.silentAfterChunks) {
									// Provider stuck mid-thinking: no more events, no abort awareness.
									await new Promise<never>(() => {});
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
	streamResult: AssistantMessageEventStream,
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

function textOf(message: AssistantMessage): string {
	return message.content
		.filter((block): block is TextContent => block.type === "text")
		.map((block) => block.text)
		.join("");
}

function zaiModel() {
	const model = getModel("zai", "glm-5.3")!;
	expect(model.compat?.thinkingFormat).toBe("zai");
	expect(model.reasoning).toBe(true);
	return model;
}

/**
 * Provider that streams thinking and then throws while unwinding the abort
 * instead of reporting a terminal event, the way an inner stream that rejects
 * reaches the wrapper. Its own limits come from `Model.reasoningLimits`.
 */
function registerThrowingThinkingProvider(chunkCount: number) {
	const api = "test-throwing-thinking";
	const sourceId = "res-283-throwing-thinking-provider";
	const chunk = "t".repeat(500);
	const state = { aborted: false };
	const model: Model<string> = {
		id: "throwing-thinking",
		name: "Throwing Thinking",
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
		reasoningLimits: { maxThinkingChars: 2_000 },
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel, _context, options) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			const block: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [block];
			for (let index = 0; index < chunkCount; index++) {
				await new Promise((resolve) => setTimeout(resolve, 1));
				block.thinking += chunk;
				yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			}
			await new Promise<void>((resolve) => {
				if (options?.signal?.aborted) {
					resolve();
					return;
				}
				options?.signal?.addEventListener("abort", () => resolve(), { once: true });
			});
			state.aborted = true;
			throw new Error("Request was aborted");
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, state, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider that thinks, moves on to text or a tool call, and only sends the
 * first output after a silence longer than the configured time budget, the way
 * a slow first token looks to the wrapper.
 */
function registerLateOutputProvider(options: { phase: "text" | "toolcall"; silenceMs: number }) {
	const api = `test-late-${options.phase}`;
	const sourceId = `res-283-late-${options.phase}-provider`;
	const chunk = "t".repeat(200);
	const state = { abortedAfterPhase: false };
	const model: Model<string> = {
		id: `late-${options.phase}`,
		name: `Late ${options.phase}`,
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel, _context, streamOptions) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			const thinking: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [thinking];
			for (let index = 0; index < 2; index++) {
				await sleep(1);
				thinking.thinking += chunk;
				yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			}

			const contentIndex = 1;
			if (options.phase === "text") {
				const block: TextContent = { type: "text", text: "" };
				partial.content = [thinking, block];
				yield { type: "text_start", contentIndex, partial };
				await sleep(options.silenceMs);
				state.abortedAfterPhase = streamOptions?.signal?.aborted ?? false;
				block.text = "design ready";
				yield { type: "text_delta", contentIndex, delta: block.text, partial };
				yield { type: "text_end", contentIndex, content: block.text, partial };
				partial.stopReason = "stop";
			} else {
				const block: ToolCall = { type: "toolCall", id: "call-late", name: "measure", arguments: {} };
				partial.content = [thinking, block];
				yield { type: "toolcall_start", contentIndex, partial };
				await sleep(options.silenceMs);
				state.abortedAfterPhase = streamOptions?.signal?.aborted ?? false;
				yield { type: "toolcall_delta", contentIndex, delta: "{}", partial };
				yield { type: "toolcall_end", contentIndex, toolCall: block, partial };
				partial.stopReason = "toolUse";
			}
			yield { type: "done", reason: options.phase === "text" ? "stop" : "toolUse", message: partial };
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, state, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider that closes its thinking block and only then goes quiet: it reports
 * `thinking_start`, deltas and `thinking_end`, then stalls past the time budget
 * before the next block starts. The thinking phase is over, so the deadline
 * must not fire during that silence.
 */
function registerEndedThinkingProvider(options: { silenceMs: number }) {
	const api = "test-ended-thinking";
	const sourceId = "res-283-ended-thinking-provider";
	const chunk = "t".repeat(200);
	const state = { abortedAfterThinkingEnd: false };
	const model: Model<string> = {
		id: "ended-thinking",
		name: "Ended Thinking",
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel, _context, streamOptions) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			const thinking: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [thinking];
			yield { type: "thinking_start", contentIndex: 0, partial };
			for (let index = 0; index < 2; index++) {
				await sleep(1);
				thinking.thinking += chunk;
				yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			}
			yield { type: "thinking_end", contentIndex: 0, content: thinking.thinking, partial };

			// The thinking block is closed; the next block has not started yet.
			await sleep(options.silenceMs);
			state.abortedAfterThinkingEnd = streamOptions?.signal?.aborted ?? false;

			const text: TextContent = { type: "text", text: "" };
			partial.content = [thinking, text];
			yield { type: "text_start", contentIndex: 1, partial };
			text.text = "design ready";
			yield { type: "text_delta", contentIndex: 1, delta: text.text, partial };
			yield { type: "text_end", contentIndex: 1, content: text.text, partial };
			yield { type: "done", reason: "stop", message: partial };
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, state, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider that is slow to unwind a caller abort: it thinks until the caller
 * stops the turn, then only answers back after a delay long enough to cross the
 * reasoning deadline. `report` picks the two ways that answer arrives, a
 * terminal event that already says `aborted`, or a throw while unwinding.
 */
function registerSlowAbortProvider(options: { unwindMs: number; report: "aborted" | "throw" }) {
	const api = `test-slow-abort-${options.report}`;
	const sourceId = `res-283-slow-abort-${options.report}-provider`;
	const chunk = "t".repeat(200);
	const state = { unwound: false };
	const model: Model<string> = {
		id: `slow-abort-${options.report}`,
		name: `Slow Abort ${options.report}`,
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel, _context, streamOptions) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			const thinking: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [thinking];
			yield { type: "thinking_start", contentIndex: 0, partial };
			for (let index = 0; index < 3; index++) {
				await sleep(10);
				thinking.thinking += chunk;
				yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			}
			await new Promise<void>((resolve) => {
				if (streamOptions?.signal?.aborted) {
					resolve();
					return;
				}
				streamOptions?.signal?.addEventListener("abort", () => resolve(), { once: true });
			});
			await sleep(options.unwindMs);
			state.unwound = true;
			if (options.report === "throw") throw new Error("Request was aborted");
			partial.stopReason = "aborted";
			partial.errorMessage = "Request was aborted";
			yield { type: "error", reason: "aborted", error: partial };
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, state, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider that streams thinking and then stops sending events entirely: it
 * never reports a terminal event and never looks at the abort signal, the way a
 * provider that is wedged on its own socket behaves. The caller can stop the
 * turn, but only a bounded settle in the wrapper can bring the turn back.
 */
function registerStalledProvider(options: { chunkCount: number }) {
	const api = "test-stalled";
	const sourceId = "res-283-stalled-provider";
	const chunk = "t".repeat(200);
	const state = { abortSignalled: false };
	const model: Model<string> = {
		id: "stalled",
		name: "Stalled",
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel, _context, streamOptions) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			// Note that the abort arrived, without ever acting on it.
			streamOptions?.signal?.addEventListener(
				"abort",
				() => {
					state.abortSignalled = true;
				},
				{ once: true },
			);
			yield { type: "start", partial };
			const thinking: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [thinking];
			yield { type: "thinking_start", contentIndex: 0, partial };
			for (let index = 0; index < options.chunkCount; index++) {
				await sleep(1);
				thinking.thinking += chunk;
				yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			}
			// Wedged: no further event, no failure, nothing to await.
			await new Promise<never>(() => {});
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, state, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider that reports its own terminal event and only closes its iterator a
 * while later, the way an inner stream that is still unwinding reaches the
 * wrapper. `onClosing` fires inside that window, so the caller can stop the turn
 * after the provider already finished it.
 */
function registerSlowClosingProvider(options: { closeMs: number; onClosing: () => void }) {
	const api = "test-slow-closing";
	const sourceId = "res-283-slow-closing-provider";
	const chunk = "t".repeat(200);
	const model: Model<string> = {
		id: "slow-closing",
		name: "Slow Closing",
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			const thinking: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [thinking];
			yield { type: "thinking_start", contentIndex: 0, partial };
			thinking.thinking += chunk;
			yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			const text: TextContent = { type: "text", text: "" };
			partial.content = [thinking, text];
			yield { type: "text_start", contentIndex: 1, partial };
			text.text = "design ready";
			yield { type: "text_delta", contentIndex: 1, delta: text.text, partial };
			partial.stopReason = "stop";

			// The turn is finished as far as the provider is concerned, but the
			// iterator stays open a while longer.
			setTimeout(() => options.onClosing(), 20);
			yield { type: "done", reason: "stop", message: partial };
			await sleep(options.closeMs);
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider that ends its own stream with the `reasoning_limit` stop reason, the
 * way an inner guard would, without this wrapper's own budget tripping.
 */
function registerPreLimitedProvider() {
	const api = "test-pre-limited";
	const sourceId = "res-283-pre-limited-provider";
	const model: Model<string> = {
		id: "pre-limited",
		name: "Pre Limited",
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
		reasoningLimits: { maxThinkingChars: 1_000_000 },
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [{ type: "thinking", thinking: "half a thought" }],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "reasoning_limit",
			reasoningLimit: { reason: "max_thinking_chars", limit: 4, observed: 4 },
			errorMessage:
				"Reasoning limit reached: 4 characters of thinking without text or a tool call (limit 4 characters).",
			timestamp: Date.now(),
		};
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			yield { type: "thinking_delta", contentIndex: 0, delta: "half a thought", partial };
			yield { type: "error", reason: "reasoning_limit", error: partial };
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, unregister: () => unregisterApiProviders(sourceId) };
}

/**
 * Provider whose turn fails on its own, with no caller abort and no budget
 * trip: it streams part of a turn and then either rejects with a plain provider
 * error or closes its iterator without ever naming a terminal event. Either
 * way the partial message is all the wrapper has to settle with, and that
 * partial still carries the provider's initial `stop`.
 */
function registerFailingProvider(options: { outcome: "throw" | "eof" }) {
	const api = `test-failing-${options.outcome}`;
	const sourceId = `res-283-failing-${options.outcome}-provider`;
	const chunk = "t".repeat(200);
	const state = { streamed: false };
	const model: Model<string> = {
		id: `failing-${options.outcome}`,
		name: `Failing ${options.outcome}`,
		api,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: true,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 128_000,
		maxTokens: 16_384,
	};
	const streamFn: StreamFunction<string, StreamOptions> = (requestModel) => {
		const outer = createAssistantMessageEventStream();
		const partial: AssistantMessage = {
			role: "assistant",
			content: [],
			api,
			provider: model.provider,
			model: requestModel.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};
		outer[Symbol.asyncIterator] = async function* (): AsyncGenerator<AssistantMessageEvent, void> {
			yield { type: "start", partial };
			const thinking: ThinkingContent = { type: "thinking", thinking: "" };
			partial.content = [thinking];
			yield { type: "thinking_start", contentIndex: 0, partial };
			thinking.thinking += chunk;
			yield { type: "thinking_delta", contentIndex: 0, delta: chunk, partial };
			const text: TextContent = { type: "text", text: "" };
			partial.content = [thinking, text];
			yield { type: "text_start", contentIndex: 1, partial };
			text.text = "design ready";
			yield { type: "text_delta", contentIndex: 1, delta: text.text, partial };
			state.streamed = true;
			if (options.outcome === "throw") throw new Error("socket hang up");
			// Closed without a terminal event, so nothing ever named the end of this turn.
		};
		return outer;
	};
	registerApiProvider({ api, stream: streamFn, streamSimple: streamFn }, sourceId);
	return { model, state, unregister: () => unregisterApiProviders(sourceId) };
}

beforeEach(() => {
	mockState.lastParams = undefined;
	mockState.lastSignal = undefined;
	mockState.usage = undefined;
	mockState.usageAfterChunks = Number.POSITIVE_INFINITY;
	mockState.maxChunks = Number.POSITIVE_INFINITY;
	mockState.silentAfterChunks = Number.POSITIVE_INFINITY;
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

	it("stops a thinking phase that goes silent on the deadline alone", async () => {
		const model = zaiModel();
		mockState.delayMs = 10;
		// Three thinking chunks, then a provider that never sends another event
		// and never notices the abort.
		mockState.silentAfterChunks = 3;

		const { message } = await collect(
			streamSimple(model, context, {
				apiKey: "test",
				reasoning: "medium",
				reasoningLimits: { maxThinkingMs: 50 },
			}),
		);

		// No further delta arrived, so only the timer could end this stream.
		expect(mockState.sawAbort).toBe(false);
		expect(mockState.lastSignal?.aborted).toBe(true);
		expect(message.stopReason).toBe("reasoning_limit");
		expect(message.reasoningLimit?.reason).toBe("max_thinking_ms");
		expect(message.reasoningLimit?.observed).toBeGreaterThanOrEqual(50);
		// Thinking that arrived before the provider stalled is kept.
		expect(thinkingOf(message)).toBe("t".repeat(1_500));
		expect(message.usageUnavailable).toBe("reasoning_limit");
	});

	it("keeps the partial thinking when an aborted provider stream throws", async () => {
		const provider = registerThrowingThinkingProvider(4);
		try {
			const { message } = await collect(stream(provider.model, context, { apiKey: "test" }));

			expect(provider.state.aborted).toBe(true);
			expect(message.stopReason).toBe("reasoning_limit");
			expect(message.reasoningLimit).toEqual({
				reason: "max_thinking_chars",
				limit: 2_000,
				observed: 2_000,
			});
			// The thinking that was already streamed survives the throw instead of
			// being replaced by an empty failure message.
			expect(message.content).toHaveLength(1);
			expect(message.content.every((block) => block.type === "thinking")).toBe(true);
			expect(thinkingOf(message)).toBe("t".repeat(2_000));
			expect(message.usage.totalTokens).toBe(0);
			expect(message.usageUnavailable).toBe("reasoning_limit");
		} finally {
			provider.unregister();
		}
	});

	it("does not treat slow output after a thinking phase as runaway thinking", async () => {
		for (const phase of ["text", "toolcall"] as const) {
			const provider = registerLateOutputProvider({ phase, silenceMs: 250 });
			try {
				const { events, message } = await collect(
					stream(provider.model, context, { apiKey: "test", reasoningLimits: { maxThinkingMs: 50 } }),
				);

				// Starting text or a tool call leaves the thinking phase, so the
				// time budget must not fire while that output is still slow.
				expect(provider.state.abortedAfterPhase).toBe(false);
				expect(message.stopReason).toBe(phase === "text" ? "stop" : "toolUse");
				expect(message.reasoningLimit).toBeUndefined();
				expect(events.some((event) => event.type === "error")).toBe(false);
			} finally {
				provider.unregister();
			}
		}
	});

	it("removes its upstream abort listener once the stream settles", async () => {
		const model = zaiModel();
		mockState.maxChunks = 2;
		const controller = new AbortController();

		// One agent run reuses the same signal for many model calls; every call
		// must clean up after itself or Node starts warning about a leak.
		for (let call = 0; call < 25; call++) {
			await collect(
				streamSimple(model, context, { apiKey: "test", reasoning: "medium", signal: controller.signal }),
			);
		}

		expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
	});

	it("does not trip the deadline after an explicit thinking_end", async () => {
		const provider = registerEndedThinkingProvider({ silenceMs: 300 });
		try {
			const { message } = await collect(
				stream(provider.model, context, { apiKey: "test", reasoningLimits: { maxThinkingMs: 100 } }),
			);

			// The thinking block was closed, so the silence before the next block
			// is not runaway reasoning and the deadline must be disarmed.
			expect(provider.state.abortedAfterThinkingEnd).toBe(false);
			expect(message.stopReason).toBe("stop");
			expect(message.reasoningLimit).toBeUndefined();
		} finally {
			provider.unregister();
		}
	});

	it("keeps a caller abort that lands before the reasoning deadline", async () => {
		for (const report of ["aborted", "throw"] as const) {
			const provider = registerSlowAbortProvider({ unwindMs: 300, report });
			try {
				const controller = new AbortController();
				const streamResult = stream(provider.model, context, {
					apiKey: "test",
					reasoningLimits: { maxThinkingMs: 100 },
					signal: controller.signal,
				});

				// The caller stops the turn while the model is still thinking, then
				// the provider only answers back well after the deadline passed.
				let thinkingDeltas = 0;
				for await (const event of streamResult) {
					if (event.type === "thinking_delta" && ++thinkingDeltas === 3) controller.abort();
				}
				const message = await streamResult.result();

				expect(provider.state.unwound).toBe(true);
				// The abort came first, so it owns the terminal reason.
				expect(message.stopReason).toBe("aborted");
				expect(message.reasoningLimit).toBeUndefined();
				expect(thinkingOf(message).length).toBeGreaterThan(0);
				expect(message.usageUnavailable).toBe("aborted");
			} finally {
				provider.unregister();
			}
		}
	});

	it("settles a caller abort even when the provider never answers the abort", async () => {
		const provider = registerStalledProvider({ chunkCount: 3 });
		try {
			const controller = new AbortController();
			const streamResult = stream(provider.model, context, {
				apiKey: "test",
				reasoningLimits: { maxThinkingMs: 60_000 },
				signal: controller.signal,
			});

			// The caller stops the turn while the provider is wedged mid-thinking.
			let thinkingDeltas = 0;
			for await (const event of streamResult) {
				if (event.type === "thinking_delta" && ++thinkingDeltas === 3) controller.abort();
			}
			const message = await streamResult.result();

			// The provider was told to stop and ignored it, so only the bounded
			// settle can bring this turn back.
			expect(provider.state.abortSignalled).toBe(true);
			expect(message.stopReason).toBe("aborted");
			expect(message.reasoningLimit).toBeUndefined();
			expect(thinkingOf(message)).toBe("t".repeat(600));
			expect(message.usageUnavailable).toBe("aborted");
		} finally {
			provider.unregister();
		}
	});

	it("keeps the provider terminal when the caller aborts while the stream is still closing", async () => {
		const controller = new AbortController();
		const provider = registerSlowClosingProvider({ closeMs: 300, onClosing: () => controller.abort() });
		try {
			const streamResult = stream(provider.model, context, {
				apiKey: "test",
				reasoningLimits: { maxThinkingMs: 60_000 },
				signal: controller.signal,
			});
			const { events, message } = await collect(streamResult);

			// The stop lands while the provider is still closing its stream, after
			// it already reported the end of the turn. First terminal wins.
			await new Promise((resolve) => setTimeout(resolve, 80));
			expect(controller.signal.aborted).toBe(true);
			expect(message.stopReason).toBe("stop");
			expect(message.reasoningLimit).toBeUndefined();
			expect(message.errorMessage).toBeUndefined();
			expect(events.some((event) => event.type === "done")).toBe(true);
			expect(events.some((event) => event.type === "error")).toBe(false);
			expect(thinkingOf(message)).toBe("t".repeat(200));
		} finally {
			provider.unregister();
		}
	});

	it("removes its upstream abort listener after the guard trips", async () => {
		const model = zaiModel();
		const controller = new AbortController();

		const { message } = await collect(
			streamSimple(model, context, { apiKey: "test", reasoning: "minimal", signal: controller.signal }),
		);

		expect(message.stopReason).toBe("reasoning_limit");
		expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
	});

	it("settles a message that already carries reasoning_limit through the error variant", async () => {
		const provider = registerPreLimitedProvider();
		try {
			const { events, message } = await collect(stream(provider.model, context, { apiKey: "test" }));

			const errorReasons = events
				.filter((event) => event.type === "error")
				.map((event) => (event.type === "error" ? event.reason : undefined));
			expect(errorReasons).toEqual(["reasoning_limit"]);
			// `reasoning_limit` is a failure reason, never a `done`.
			expect(events.some((event) => event.type === "done")).toBe(false);
			expect(message.stopReason).toBe("reasoning_limit");
			expect(message.usageUnavailable).toBe("reasoning_limit");
		} finally {
			provider.unregister();
		}
	});

	it("settles a plain provider failure as an error even when a partial turn arrived", async () => {
		const provider = registerFailingProvider({ outcome: "throw" });
		try {
			const { events, message } = await collect(
				stream(provider.model, context, { apiKey: "test", reasoningLimits: { maxThinkingChars: 1_000_000 } }),
			);

			// A rejected iterator is a failed turn, not a finished one: the partial
			// message still carries the provider's initial `stop`, and reporting
			// that as a success would hide the failure from the caller.
			expect(provider.state.streamed).toBe(true);
			expect(events.some((event) => event.type === "done")).toBe(false);
			expect(events.filter((event) => event.type === "error").map((event) => event.reason)).toEqual(["error"]);
			expect(message.stopReason).toBe("error");
			expect(message.errorMessage).toBe("socket hang up");
			// What the provider streamed before failing survives.
			expect(thinkingOf(message)).toBe("t".repeat(200));
			expect(textOf(message)).toBe("design ready");
			expect(message.usageUnavailable).toBe("error");
		} finally {
			provider.unregister();
		}
	});

	it("settles a stream that closes without a terminal event as an error", async () => {
		const provider = registerFailingProvider({ outcome: "eof" });
		try {
			const { events, message } = await collect(
				stream(provider.model, context, { apiKey: "test", reasoningLimits: { maxThinkingChars: 1_000_000 } }),
			);

			// Closing the iterator is not a terminal event either, so the partial
			// must not be read as a successful `stop`.
			expect(provider.state.streamed).toBe(true);
			expect(events.some((event) => event.type === "done")).toBe(false);
			expect(events.filter((event) => event.type === "error").map((event) => event.reason)).toEqual(["error"]);
			expect(message.stopReason).toBe("error");
			expect(message.errorMessage).toBe("Provider stream ended without a terminal event");
			expect(thinkingOf(message)).toBe("t".repeat(200));
			expect(textOf(message)).toBe("design ready");
			expect(message.usageUnavailable).toBe("error");
		} finally {
			provider.unregister();
		}
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
