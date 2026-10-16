import { createReadStream, fstatSync } from "node:fs";
import type { Readable } from "node:stream";

const SESSION_ISSUER_FD_ENV = "PI_CAD_SESSION_ISSUER_FD";
const SESSION_ISSUER_CAPABILITY_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const SESSION_ISSUER_READ_TIMEOUT_MS = 2_000;

let pendingSessionIssuerCapability: string | undefined;

export function readSessionIssuerBootstrapFrame(input: Readable, timeoutMs = SESSION_ISSUER_READ_TIMEOUT_MS): Promise<string> {
	return new Promise((resolve, reject) => {
		let buffered = Buffer.alloc(0);
		const finish = (error?: Error, capability?: string) => {
			clearTimeout(timeout);
			input.off("data", onData);
			input.off("end", onEnd);
			input.off("error", onError);
			input.pause();
			if (error) reject(error);
			else resolve(capability!);
		};
		const timeout = setTimeout(() => finish(new Error("Prime session issuer bootstrap timed out")), timeoutMs);
		const onData = (chunk: Buffer | string) => {
			const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
			const newline = bytes.indexOf(0x0a);
			const bodyEnd = newline === -1 ? bytes.length : newline;
			if (buffered.length + bodyEnd > 43 || (newline !== -1 && buffered.length + bodyEnd !== 43)) {
				finish(new Error("Prime session issuer bootstrap has an invalid frame length"));
				return;
			}
			buffered = Buffer.concat([buffered, bytes.subarray(0, bodyEnd)]);
			if (newline !== -1) {
				if (newline !== bytes.length - 1 || !SESSION_ISSUER_CAPABILITY_PATTERN.test(buffered.toString("ascii"))) {
					finish(new Error("Prime session issuer bootstrap has an invalid format"));
					return;
				}
				finish(undefined, buffered.toString("ascii"));
			}
		};
		const onEnd = () => finish(new Error("Prime session issuer bootstrap ended before its frame"));
		const onError = (error: Error) => finish(error);
		input.on("data", onData);
		input.once("end", onEnd);
		input.once("error", onError);
		input.resume();
	});
}

/** Consume the one-shot host bootstrap before the CLI starts its daemon client. */
export async function consumeSessionIssuerBootstrap(environment: NodeJS.ProcessEnv = process.env): Promise<void> {
	const rawFd = environment[SESSION_ISSUER_FD_ENV];
	delete environment[SESSION_ISSUER_FD_ENV];
	if (rawFd === undefined) return;
	if (!/^\d+$/.test(rawFd)) throw new Error("Prime session issuer bootstrap descriptor is invalid");
	const fd = Number(rawFd);
	if (!Number.isSafeInteger(fd) || fd < 3 || fd > 255) {
		throw new Error("Prime session issuer bootstrap descriptor is invalid");
	}

	let input: ReturnType<typeof createReadStream> | undefined;
	try {
		const descriptorStat = fstatSync(fd);
		if (!descriptorStat.isFIFO() && !descriptorStat.isSocket()) {
			throw new Error("Prime session issuer bootstrap must be a pipe");
		}
		input = createReadStream(`/dev/fd/${fd}`, { fd, autoClose: true });
		pendingSessionIssuerCapability = await readSessionIssuerBootstrapFrame(input, SESSION_ISSUER_READ_TIMEOUT_MS);
	} finally {
		input?.destroy();
	}
}

/** Transfer the issuer only to the first create request made by this CLI process. */
export function takeSessionIssuerCapability(): string | undefined {
	const capability = pendingSessionIssuerCapability;
	pendingSessionIssuerCapability = undefined;
	return capability;
}
