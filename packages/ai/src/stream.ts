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
	StreamOptions,
} from "./types.js";
import { AssistantMessageEventStream, terminalAssistantEvent } from "./utils/event-stream.js";
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
 * aborted, either by the reasoning limit or by the caller. Providers that
 * observe the abort report the partial message themselves, usually with real
 * usage; one that keeps sitting on the stream must not keep the caller waiting
 * for it.
 */
const ABORT_SETTLE_GRACE_MS = 500;

/**
 * Events that prove the model left the thinking phase: either the thinking
 * block was closed explicitly, or text / tool output began. Any of them ends
 * the phase, so a provider that goes quiet afterwards is not mistaken for
 * runaway reasoning.
 */
const THINKING_PHASE_END_EVENTS = new Set<AssistantMessageEvent["type"]>([
	"thinking_end",
	"text_start",
	"text_delta",
	"toolcall_start",
	"toolcall_delta",
]);

/**
 * Stop reasons that already name a failure. A partial that carries one of them
 * describes its own end, so settling it keeps that reason.
 */
const FAILURE_STOP_REASONS = new Set<AssistantMessage["stopReason"]>(["error", "aborted", "reasoning_limit"]);

/**
 * Enforce the local thinking budget around one provider stream.
 *
 * Providers whose request format is a plain thinking switch cannot honour the
 * requested thinking level, so the kernel watches the thinking that arrives
 * without any text or tool output and aborts the provider stream once a
 * configured limit is reached. Character and token limits are checked as
 * deltas arrive, the time limit is a real deadline so a provider that goes
 * quiet mid-thinking is stopped too, and reaching text or a tool call ends the
 * phase so slow output is never mistaken for runaway thinking. Whatever
 * partial message the provider already produced is kept. The stop reason
 * becomes `reasoning_limit`, and usage that was never reported is marked
 * unavailable instead of being recorded as a real zero.
 *
 * A caller abort outranks the deadline: whoever asked to stop first owns the
 * terminal reason, so the deadline is disarmed as soon as the upstream signal
 * aborts and a slow provider that only answers back afterwards still settles as
 * `aborted`. A provider that never answers back must not park the caller, so an
 * abort also arms the same bounded settle the limit path uses. The reverse
 * order matters just as much: a terminal the provider already reported wins,
 * and a stop that arrives while that stream is still closing is ignored.
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
	const guard = new ReasoningRunawayGuard(resolved.limits);
	const inner = run({ ...(options ?? {}), signal: controller.signal } as StreamOptions);
	const stream = new AssistantMessageEventStream();
	let trip: ReasoningLimitDetails | undefined;
	/** Newest partial the provider reported, kept for the abort and error paths. */
	let partial: AssistantMessage | undefined;
	let settled = false;
	/** Set when the caller's own signal aborted; that decision beats the deadline. */
	let upstreamAborted = false;
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
		cleanup();
		if (upstreamAborted) {
			// The caller aborted while the provider was still unwinding, so the
			// provider never labelled its own message. Keep what it streamed and
			// report the abort instead of a success or a reasoning limit.
			message.stopReason = "aborted";
			message.reasoningLimit = undefined;
			markUsageUnavailable(message, "aborted");
			stream.push({ type: "error", reason: "aborted", error: message });
			stream.end(message);
			return;
		}
		if (trip || message.stopReason === "reasoning_limit") {
			const details = trip ?? message.reasoningLimit;
			message.stopReason = "reasoning_limit";
			if (details) {
				message.reasoningLimit = details;
				message.errorMessage = formatReasoningLimitMessage(details);
			} else if (!message.errorMessage) {
				message.errorMessage = "Reasoning limit reached.";
			}
			markUsageUnavailable(message, "reasoning_limit");
			stream.push({ type: "error", reason: "reasoning_limit", error: message });
			stream.end(message);
			return;
		}
		const terminal = terminalAssistantEvent(message);
		if (terminal.type === "error") markUsageUnavailable(message, terminal.reason);
		stream.push(terminal);
		stream.end(message);
	};

	/**
	 * Settle with what the provider already streamed. An abort or a provider
	 * error must not replace the partial thinking with an empty failure message.
	 *
	 * This only runs when the provider never named its own terminal, so the
	 * partial must not be allowed to report a success: it still carries the
	 * provider's initial `stop` until the turn really ends, and passing that
	 * straight to `settle` would turn a plain failure - a rejected iterator, a
	 * stream closed without a terminal event - into a completed turn. Keep the
	 * partial content and usage, and label the message as an error instead. The
	 * abort and limit paths are settled by `settle` itself, which owns their
	 * terminal reason.
	 */
	const settleWithPartial = (fallbackError: string): void => {
		// The loop settles and breaks as soon as the provider names its own
		// terminal, and the settle below still runs once after that break. Leave
		// a finished turn alone.
		if (settled) return;
		const message = partial ?? failedMessage(model, fallbackError);
		if (!upstreamAborted && !trip && !FAILURE_STOP_REASONS.has(message.stopReason)) {
			message.stopReason = "error";
			if (!message.errorMessage) message.errorMessage = fallbackError;
		}
		settle(message);
	};

	/**
	 * Give the provider a bounded moment to report its own final message, then
	 * settle with what already arrived. Without this a provider that ignores the
	 * abort would leave the caller waiting on a stream that never ends.
	 */
	const armSettleGrace = (fallbackError: string): void => {
		if (graceTimer !== undefined) return;
		graceTimer = setTimeout(() => settleWithPartial(fallbackError), ABORT_SETTLE_GRACE_MS);
	};

	const detachUpstreamAbort = (): void => {
		upstreamSignal?.removeEventListener("abort", forwardAbort);
	};

	const cleanup = (): void => {
		clearTimers();
		detachUpstreamAbort();
	};

	// Named so it can be removed when the stream settles: one agent run reuses
	// the same signal for many model calls, and anonymous listeners would pile
	// up until Node starts warning about a leak.
	const forwardAbort = (): void => {
		// The caller stopped the turn. Record that before anything else so the
		// deadline cannot fire while the provider unwinds and rewrite the turn
		// as a reasoning limit, and drop the pending deadline here rather than
		// waiting for the provider to come back.
		upstreamAborted = true;
		clearTimers();
		controller.abort();
		armSettleGrace("Caller aborted the stream.");
	};
	if (upstreamSignal) {
		if (upstreamSignal.aborted) {
			forwardAbort();
		} else {
			upstreamSignal.addEventListener("abort", forwardAbort, { once: true });
		}
	}

	/** Stop the provider stream and give it a bounded moment to report its own final message. */
	const abortForLimit = (details: ReasoningLimitDetails): void => {
		if (upstreamAborted) return;
		trip = details;
		clearTimers();
		detachUpstreamAbort();
		controller.abort();
		armSettleGrace(formatReasoningLimitMessage(details));
	};

	function onPhaseDeadline(): void {
		phaseTimer = undefined;
		if (settled || trip || upstreamAborted) return;
		const details = guard.check();
		if (details) {
			abortForLimit(details);
			return;
		}
		// Timers can fire marginally early; wait out the remainder of the phase.
		phaseTimer = setTimeout(onPhaseDeadline, Math.max(1, guard.remainingMs ?? 1));
	}

	function armPhaseDeadline(): void {
		if (upstreamAborted) return;
		const remaining = guard.remainingMs;
		if (remaining === undefined || phaseTimer !== undefined) return;
		phaseTimer = setTimeout(onPhaseDeadline, Math.max(1, remaining));
	}

	void (async () => {
		try {
			for await (const event of inner) {
				if (settled) break;
				if (event.type === "done" || event.type === "error") {
					// The provider named the end of this turn, so settle it right here.
					// Only waiting for the iterator to close would leave a window where a
					// caller abort that arrives while the stream is still unwinding
					// rewrites a finished turn as `aborted`.
					settle(event.type === "done" ? event.message : event.error);
					break;
				}
				// The newest partial is kept even after the budget is spent: the
				// provider may have buffered slightly ahead and those blocks are
				// still what the caller should end up with.
				partial = event.partial;
				// Once the budget is spent the stream is already terminating;
				// stop delivering further deltas so callers never see thinking
				// beyond the limit.
				if (trip) continue;
				if (!upstreamAborted) {
					if (event.type === "thinking_start" || event.type === "thinking_delta") {
						const thinking = guard.isThinking;
						const details = guard.observeThinking(event.type === "thinking_delta" ? event.delta.length : 0);
						if (details) {
							abortForLimit(details);
						} else if (!thinking) {
							armPhaseDeadline();
						}
					} else if (THINKING_PHASE_END_EVENTS.has(event.type)) {
						guard.reset();
						clearTimers();
					}
				}
				stream.push(event);
			}
		} catch (error) {
			settleWithPartial(error instanceof Error ? error.message : String(error));
			return;
		}

		// A terminal event settles above; reaching here means the stream closed
		// without naming an end, or the caller already settled it.
		settleWithPartial("Provider stream ended without a terminal event");
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
