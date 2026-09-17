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
 * Time a provider gets to hand back its own final message after the stream was
 * aborted for a reasoning limit. Providers that observe the abort report the
 * partial message themselves, usually with real usage; one that keeps sitting
 * on the stream must not keep the caller waiting for it.
 */
const REASONING_LIMIT_SETTLE_GRACE_MS = 500;

/**
 * Enforce the local thinking budget around one provider stream.
 *
 * Providers whose request format is a plain thinking switch cannot honour the
 * requested thinking level, so the kernel watches the thinking that arrives
 * without any text or tool output and aborts the provider stream once a
 * configured limit is reached. Character and token limits are checked as
 * deltas arrive, the time limit is a real deadline so a provider that goes
 * quiet mid-thinking is stopped too, and whatever partial message the provider
 * already produced is kept. The stop reason becomes `reasoning_limit`, and
 * usage that was never reported is marked unavailable instead of being
 * recorded as a real zero.
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
	/** Newest partial the provider reported, kept for the abort and error paths. */
	let partial: AssistantMessage | undefined;
	let settled = false;
	/** Deadline for the open thinking phase, so a silent provider still stops. */
	let phaseTimer: ReturnType<typeof setTimeout> | undefined;
	let graceTimer: ReturnType<typeof setTimeout> | undefined;

	const clearTimers = (): void => {
		if (phaseTimer !== undefined) {
			clearTimeout(phaseTimer);
			phaseTimer = undefined;
		}
		if (graceTimer !== undefined) {
			clearTimeout(graceTimer);
			graceTimer = undefined;
		}
	};

	const settle = (message: AssistantMessage): void => {
		if (settled) return;
		settled = true;
		clearTimers();
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

	/**
	 * Settle with what the provider already streamed. An abort or a provider
	 * error must not replace the partial thinking with an empty failure message.
	 */
	const settleWithPartial = (fallbackError: string): void => {
		settle(partial ?? failedMessage(model, fallbackError));
	};

	/** Stop the provider stream and give it a bounded moment to report its own final message. */
	const abortForLimit = (details: ReasoningLimitDetails): void => {
		trip = details;
		clearTimers();
		controller.abort();
		graceTimer = setTimeout(
			() => settleWithPartial(formatReasoningLimitMessage(details)),
			REASONING_LIMIT_SETTLE_GRACE_MS,
		);
	};

	function onPhaseDeadline(): void {
		phaseTimer = undefined;
		if (settled || trip) return;
		const details = guard.check();
		if (details) {
			abortForLimit(details);
			return;
		}
		// Timers can fire marginally early; wait out the remainder of the phase.
		phaseTimer = setTimeout(onPhaseDeadline, Math.max(1, guard.remainingMs ?? 1));
	}

	function armPhaseDeadline(): void {
		const remaining = guard.remainingMs;
		if (remaining === undefined || phaseTimer !== undefined) return;
		phaseTimer = setTimeout(onPhaseDeadline, Math.max(1, remaining));
	}

	void (async () => {
		let terminal: AssistantMessageEvent | undefined;
		try {
			for await (const event of inner) {
				if (settled) break;
				if (event.type === "done" || event.type === "error") {
					terminal = event;
					continue;
				}
				// The newest partial is kept even after the budget is spent: the
				// provider may have buffered slightly ahead and those blocks are
				// still what the caller should end up with.
				partial = event.partial;
				// Once the budget is spent the stream is already terminating;
				// stop delivering further deltas so callers never see thinking
				// beyond the limit.
				if (trip) continue;
				if (event.type === "thinking_start" || event.type === "thinking_delta") {
					const thinking = guard.isThinking;
					const details = guard.observeThinking(event.type === "thinking_delta" ? event.delta.length : 0);
					if (details) {
						abortForLimit(details);
					} else if (!thinking) {
						armPhaseDeadline();
					}
				} else if (event.type === "text_delta" || event.type === "toolcall_delta") {
					guard.reset();
					clearTimers();
				}
				stream.push(event);
			}
		} catch (error) {
			settleWithPartial(error instanceof Error ? error.message : String(error));
			return;
		}

		const message =
			terminal?.type === "done" ? terminal.message : terminal?.type === "error" ? terminal.error : undefined;
		if (!message) {
			settleWithPartial("Provider stream ended without a terminal event");
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
