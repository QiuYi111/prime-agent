import type { AssistantMessage, Usage, UsageUnavailableReason } from "../types.js";

/**
 * True when a message carries no measured usage at all.
 *
 * Providers that report usage with their final chunk (most OpenAI-compatible
 * endpoints, Anthropic, ...) never deliver it when the stream ends early, so
 * the placeholder zeros must not be reported as a real zero-token turn.
 */
export function isUsageUnknown(usage: Usage | undefined): boolean {
	if (!usage) return true;
	return (
		usage.input === 0 &&
		usage.output === 0 &&
		usage.cacheRead === 0 &&
		usage.cacheWrite === 0 &&
		usage.totalTokens === 0
	);
}

/**
 * Mark placeholder usage as unavailable unless the provider already reported
 * real numbers. Returns true when the message was marked.
 */
export function markUsageUnavailable(message: AssistantMessage, reason: UsageUnavailableReason): boolean {
	if (!isUsageUnknown(message.usage)) return false;
	message.usageUnavailable = reason;
	return true;
}
