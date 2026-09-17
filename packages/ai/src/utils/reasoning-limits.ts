import type {
	Api,
	Model,
	ModelThinkingLevel,
	OpenAICompletionsCompat,
	ReasoningLimitDetails,
	ReasoningLimits,
	ThinkingLevel,
} from "../types.js";

/**
 * Thinking budgets the kernel enforces itself for providers that only expose a
 * boolean thinking switch (z.ai, qwen, and friends). Without these, `minimal`
 * through `max` all collapse to "thinking on" and one turn can spend the whole
 * output budget on reasoning.
 *
 * `medium` is the product default and must stay far below the runaway case
 * (~121,000 characters of thinking over ~17 minutes) while still leaving room
 * for a genuinely hard problem.
 */
export const LOCAL_THINKING_LEVEL_LIMITS: Record<ThinkingLevel, ReasoningLimits> = {
	minimal: { maxThinkingChars: 8_000, maxThinkingMs: 60_000 },
	low: { maxThinkingChars: 24_000, maxThinkingMs: 150_000 },
	medium: { maxThinkingChars: 64_000, maxThinkingMs: 300_000 },
	high: { maxThinkingChars: 128_000, maxThinkingMs: 600_000 },
	xhigh: { maxThinkingChars: 192_000, maxThinkingMs: 900_000 },
	max: { maxThinkingChars: 256_000, maxThinkingMs: 1_200_000 },
};

/** Level used for the local budget when the request does not name one. */
export const DEFAULT_LOCAL_THINKING_LEVEL: ThinkingLevel = "medium";

export type ReasoningCapability =
	/** Model has no reasoning at all. */
	| { kind: "none" }
	/** Provider exposes a real reasoning budget (token budgets, reasoning_effort, ...). */
	| { kind: "provider" }
	/** Provider only switches thinking on or off; the kernel supplies the budget. */
	| { kind: "local"; levelLimits: Record<ThinkingLevel, ReasoningLimits> };

function openAICompat(model: Model<Api>): OpenAICompletionsCompat | undefined {
	return (model as { compat?: OpenAICompletionsCompat }).compat;
}

/** Request formats that only switch thinking on or off. */
const BINARY_THINKING_FORMATS = new Set(["zai", "zai-preserved", "qwen", "qwen-chat-template"]);

/**
 * True when the model's request format is a plain thinking switch. Such a model
 * cannot honour `minimal`..`max` on the wire, so the levels must be realised by
 * local budgets instead of being silently ignored.
 */
export function hasBinaryThinkingFormat<TApi extends Api>(model: Model<TApi>): boolean {
	if (!model.reasoning) return false;
	const compat = openAICompat(model);
	if (!compat) return false;
	if (compat.supportsReasoningEffort === true) return false;
	// String comparison keeps this working for formats added by provider
	// metadata before the type union catches up (for example `zai-preserved`).
	return typeof compat.thinkingFormat === "string" && BINARY_THINKING_FORMATS.has(compat.thinkingFormat);
}

export function getReasoningCapability<TApi extends Api>(model: Model<TApi>): ReasoningCapability {
	if (!model.reasoning) return { kind: "none" };
	if (hasBinaryThinkingFormat(model)) {
		return { kind: "local", levelLimits: LOCAL_THINKING_LEVEL_LIMITS };
	}
	return { kind: "provider" };
}

export interface ReasoningLimitOptions {
	reasoningLimits?: ReasoningLimits | false;
	/** `SimpleStreamOptions.reasoning`. */
	reasoning?: ModelThinkingLevel;
	/** `OpenAICompletionsOptions.reasoningEffort` and friends. */
	reasoningEffort?: ThinkingLevel;
}

export interface ResolvedReasoningLimits {
	limits: ReasoningLimits;
	source: "request" | "model" | "local";
	/** Level the local budget was derived from, when `source` is `local`. */
	level?: ThinkingLevel;
}

function compactLimits(limits: ReasoningLimits): ReasoningLimits {
	const compacted: ReasoningLimits = {};
	if (limits.maxThinkingChars !== undefined && limits.maxThinkingChars > 0) {
		compacted.maxThinkingChars = limits.maxThinkingChars;
	}
	if (limits.maxThinkingTokens !== undefined && limits.maxThinkingTokens > 0) {
		compacted.maxThinkingTokens = limits.maxThinkingTokens;
	}
	if (limits.maxThinkingMs !== undefined && limits.maxThinkingMs > 0) {
		compacted.maxThinkingMs = limits.maxThinkingMs;
	}
	return compacted;
}

function requestedLevel(options: ReasoningLimitOptions | undefined): ThinkingLevel | undefined {
	const level = options?.reasoning ?? options?.reasoningEffort;
	if (level === undefined || level === "off") return undefined;
	return level;
}

/**
 * Decide which local thinking limits apply to one request.
 *
 * Precedence: explicit request limits > per-model limits > level-based local
 * budget for binary-thinking providers. `reasoningLimits: false` disables the
 * guard entirely.
 */
export function resolveReasoningLimits<TApi extends Api>(
	model: Model<TApi>,
	options?: ReasoningLimitOptions,
): ResolvedReasoningLimits | undefined {
	const requested = options?.reasoningLimits;
	if (requested === false) return undefined;
	if (requested) {
		const limits = compactLimits(requested);
		return Object.keys(limits).length > 0 ? { limits, source: "request" } : undefined;
	}
	if (model.reasoningLimits) {
		const limits = compactLimits(model.reasoningLimits);
		if (Object.keys(limits).length > 0) return { limits, source: "model" };
	}

	const capability = getReasoningCapability(model);
	if (capability.kind !== "local") return undefined;

	// An explicit "off" cannot run away. An unspecified level keeps the
	// provider default, which is thinking on for these models, so it still gets
	// the default budget.
	if (options?.reasoning === "off") return undefined;

	const level = requestedLevel(options) ?? DEFAULT_LOCAL_THINKING_LEVEL;
	const limits = capability.levelLimits[level] ?? capability.levelLimits[DEFAULT_LOCAL_THINKING_LEVEL];
	return { limits, source: "local", level };
}

/**
 * Tracks one uninterrupted thinking phase. Text or tool output resets the
 * counters, so a model that thinks a little, acts, and thinks again is not
 * punished for its total thinking across a turn.
 */
export class ReasoningRunawayGuard {
	private thinkingChars = 0;
	private thinkingStartedAt: number | null = null;

	constructor(
		private readonly limits: ReasoningLimits,
		private readonly now: () => number = Date.now,
	) {}

	/** Call for every thinking delta. Returns the tripped limit, if any. */
	observeThinking(deltaLength: number): ReasoningLimitDetails | undefined {
		const at = this.now();
		if (this.thinkingStartedAt === null) this.thinkingStartedAt = at;
		if (deltaLength > 0) this.thinkingChars += deltaLength;
		return this.evaluate(at);
	}

	/** Call for text or tool output; starts a fresh thinking phase. */
	reset(): void {
		this.thinkingChars = 0;
		this.thinkingStartedAt = null;
	}

	/** True while a thinking phase is open, meaning no text or tool output arrived since it started. */
	get isThinking(): boolean {
		return this.thinkingStartedAt !== null;
	}

	/**
	 * Milliseconds left in the current thinking phase before the time limit
	 * trips. `undefined` when the limits have no time limit or no phase is open.
	 * Callers use this to arm a real deadline instead of waiting for the next
	 * delta, so a provider that goes quiet mid-thinking is still stopped.
	 */
	get remainingMs(): number | undefined {
		const { maxThinkingMs } = this.limits;
		if (maxThinkingMs === undefined || this.thinkingStartedAt === null) return undefined;
		return Math.max(0, this.thinkingStartedAt + maxThinkingMs - this.now());
	}

	/** Evaluate the open phase without new output; used by the deadline timer. */
	check(): ReasoningLimitDetails | undefined {
		return this.evaluate(this.now());
	}

	get observedChars(): number {
		return this.thinkingChars;
	}

	get observedTokens(): number {
		return Math.ceil(this.thinkingChars / 4);
	}

	get observedMs(): number {
		return this.thinkingStartedAt === null ? 0 : this.now() - this.thinkingStartedAt;
	}

	private evaluate(at: number): ReasoningLimitDetails | undefined {
		const { maxThinkingChars, maxThinkingTokens, maxThinkingMs } = this.limits;
		if (maxThinkingChars !== undefined && this.thinkingChars >= maxThinkingChars) {
			return { reason: "max_thinking_chars", limit: maxThinkingChars, observed: this.thinkingChars };
		}
		const tokens = this.observedTokens;
		if (maxThinkingTokens !== undefined && tokens >= maxThinkingTokens) {
			return { reason: "max_thinking_tokens", limit: maxThinkingTokens, observed: tokens };
		}
		const elapsed = this.thinkingStartedAt === null ? 0 : at - this.thinkingStartedAt;
		if (maxThinkingMs !== undefined && elapsed >= maxThinkingMs) {
			return { reason: "max_thinking_ms", limit: maxThinkingMs, observed: elapsed };
		}
		return undefined;
	}
}

const DETAIL_UNITS: Record<ReasoningLimitDetails["reason"], string> = {
	max_thinking_chars: "characters",
	max_thinking_tokens: "estimated tokens",
	max_thinking_ms: "ms",
};

export function formatReasoningLimitMessage(details: ReasoningLimitDetails): string {
	const unit = DETAIL_UNITS[details.reason];
	return `Reasoning limit reached: ${details.observed} ${unit} of thinking without text or a tool call (limit ${details.limit} ${unit}).`;
}
