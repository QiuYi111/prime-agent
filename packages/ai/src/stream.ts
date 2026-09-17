import "./providers/register-builtins.js";

import { getApiProvider } from "./api-registry.js";
import type {
	Api,
	AssistantMessage,
	AssistantMessageEvent,
	Context,
	Model,
	ProviderStreamOptions,
	ReasoningLimitDetails,
	SimpleStreamOptions,
	StopReason,
	StreamOptions,
} from "./types.js";
import { AssistantMessageEventStream } from "./utils/event-stream.js";
import {
	formatReasoningLimitMessage,
	type ReasoningLimitOptions,
	ReasoningRunawayGuard,
	resolveReasoningLimits,
} from "./utils/reasoning-limits.js";
import { markUsageUnavailable } from "./utils/usage.js";

export { getEnvApiKey } from "./env-api-keys.js";

function resolveApiProvider(api: Api) {
	const provider = getApiProvider(api);
	if (!provider) {
		throw new Error(`No API provider registered for api: ${api}`);
	}
	return provider;
}

function failedMessage<TApi extends Api>(model: Model<TApi>, errorMessage: string): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "error",
		errorMessage,
		timestamp: Date.now(),
	};
}

/**
 * Enforce the local thinking budget around one provider stream.
 *
 * Providers whose request format is a plain thinking switch cannot honour the
 * requested thinking level, so the kernel watches the thinking that arrives
 * without any text or tool output and aborts the provider stream once a
 * configured limit is reached. The partial message is kept, the stop reason
 * becomes `reasoning_limit`, and usage that was never reported is marked
 * unavailable instead of being recorded as a real zero.
 */
function withReasoningLimits<TApi extends Api>(
	model: Model<TApi>,
	options: StreamOptions | SimpleStreamOptions | undefined,
	run: (options: StreamOptions) => AssistantMessageEventStream,
): AssistantMessageEventStream {
	const resolved = resolveReasoningLimits(model, options as ReasoningLimitOptions | undefined);
	if (!resolved) return run(options as StreamOptions);

	const controller = new AbortController();
	const upstreamSignal = options?.signal;
	if (upstreamSignal) {
		if (upstreamSignal.aborted) {
			controller.abort();
		} else {
			upstreamSignal.addEventListener("abort", () => controller.abort(), { once: true });
		}
	}

	const guard = new ReasoningRunawayGuard(resolved.limits);
	const inner = run({ ...(options ?? {}), signal: controller.signal } as StreamOptions);
	const stream = new AssistantMessageEventStream();
	let trip: ReasoningLimitDetails | undefined;

	const settle = (message: AssistantMessage): void => {
		if (trip) {
			message.stopReason = "reasoning_limit";
			message.reasoningLimit = trip;
			message.errorMessage = formatReasoningLimitMessage(trip);
			markUsageUnavailable(message, "reasoning_limit");
			stream.push({ type: "error", reason: "reasoning_limit", error: message });
			stream.end(message);
			return;
		}
		if (message.stopReason === "aborted" || message.stopReason === "error") {
			markUsageUnavailable(message, message.stopReason);
			stream.push({ type: "error", reason: message.stopReason, error: message });
			stream.end(message);
			return;
		}
		stream.push({
			type: "done",
			reason: message.stopReason as Exclude<StopReason, "error" | "aborted">,
			message,
		});
		stream.end(message);
	};

	void (async () => {
		let terminal: AssistantMessageEvent | undefined;
		try {
			for await (const event of inner) {
				if (event.type === "done" || event.type === "error") {
					terminal = event;
					continue;
				}
				// Once the budget is spent the stream is already terminating;
				// stop delivering further deltas so callers never see thinking
				// beyond the limit. The provider's partial message (which may
				// buffer slightly ahead) is still kept in the terminal event.
				if (trip) continue;
				if (event.type === "thinking_delta") {
					trip = guard.observeThinking(event.delta.length);
					if (trip) controller.abort();
				} else if (event.type === "text_delta" || event.type === "toolcall_delta") {
					guard.reset();
				}
				stream.push(event);
			}
		} catch (error) {
			settle(failedMessage(model, error instanceof Error ? error.message : String(error)));
			return;
		}

		const message =
			terminal?.type === "done" ? terminal.message : terminal?.type === "error" ? terminal.error : undefined;
		if (!message) {
			settle(failedMessage(model, "Provider stream ended without a terminal event"));
			return;
		}
		settle(message);
	})();

	return stream;
}

export function stream<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: ProviderStreamOptions,
): AssistantMessageEventStream {
	const provider = resolveApiProvider(model.api);
	return withReasoningLimits(model, options, (guarded) => provider.stream(model, context, guarded));
}

export async function complete<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: ProviderStreamOptions,
): Promise<AssistantMessage> {
	const s = stream(model, context, options);
	return s.result();
}

export function streamSimple<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: SimpleStreamOptions,
): AssistantMessageEventStream {
	const provider = resolveApiProvider(model.api);
	return withReasoningLimits(model, options, (guarded) =>
		provider.streamSimple(model, context, guarded as SimpleStreamOptions),
	);
}

export async function completeSimple<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: SimpleStreamOptions,
): Promise<AssistantMessage> {
	const s = streamSimple(model, context, options);
	return s.result();
}
