import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ReplKernelManager } from "../src/core/kernel/index.js";
import { createTestExtensionsResult, createTestSession } from "./utilities.js";

function resolvePython(): string | undefined {
	const candidates = [
		process.env.PRIME_AGENT_KERNEL_PYTHON,
		resolve(__dirname, "..", "..", "..", "prime-agent-runtime", ".venv", "bin", "python"),
		join(homedir(), ".prime", "agent", "kernel-venv", "bin", "python"),
	].filter((candidate): candidate is string => Boolean(candidate));
	return candidates.find(existsSync);
}

const python = resolvePython();
const describeWithPython = python ? describe : describe.skip;

describeWithPython("extension kernel host request identity (real Prime sessions)", () => {
	const dir = mkdtempSync(join(tmpdir(), "prime-host-session-e2e-"));
	let firstKernel: ReplKernelManager | undefined;
	let secondKernel: ReplKernelManager | undefined;
	let firstSession: ReturnType<typeof createTestSession> | undefined;
	let secondSession: ReturnType<typeof createTestSession> | undefined;

	afterEach(async () => {
		await firstKernel?.shutdown({ snapshot: true, drainHostRequests: true });
		await secondKernel?.shutdown({ snapshot: true, drainHostRequests: true });
		firstSession?.cleanup();
		secondSession?.cleanup();
		rmSync(dir, { recursive: true, force: true });
	});

	it("binds each real Python kernel request to its owning Prime session", async () => {
		const extensionsResult = await createTestExtensionsResult([
			(pi) => {
				pi.registerKernelHostRequest({
					type: "reify.test_session_identity",
					handler: (payload, ctx) => {
						const sessionId = ctx.sessionManager.getSessionId();
						if (payload.sessionId !== sessionId) throw new Error("session identity mismatch");
						return { sessionId };
					},
				});
			},
		]);
		firstSession = createTestSession({ extensionsResult });
		secondSession = createTestSession({ extensionsResult });
		const getHandlers = (session: NonNullable<typeof firstSession>) =>
			(session.session as unknown as { _createKernelHostHandlers(): Record<string, (payload: any) => Promise<any>> })
				._createKernelHostHandlers();
		const firstId = firstSession.sessionManager.getSessionId();
		const secondId = secondSession.sessionManager.getSessionId();
		expect(firstId).not.toBe(secondId);
		firstKernel = new ReplKernelManager({ python: python as string, cwd: dir, hostHandlers: getHandlers(firstSession) });
		secondKernel = new ReplKernelManager({
			python: python as string,
			cwd: dir,
			hostHandlers: getHandlers(secondSession),
		});

		const firstOwn = await firstKernel.execute(
			`import rlm\nreply = await rlm.host_request('reify.test_session_identity', {'sessionId': ${JSON.stringify(firstId)}})\nreply['sessionId']`,
		);
		expect(firstOwn.status).toBe("ok");
		expect(firstOwn.result).toContain(firstId);

		const crossRead = await firstKernel.execute(
			`import rlm\nawait rlm.host_request('reify.test_session_identity', {'sessionId': ${JSON.stringify(secondId)}})`,
		);
		expect(crossRead.status).toBe("error");
		expect(crossRead.error?.evalue).toContain("session identity mismatch");

		const secondOwn = await secondKernel.execute(
			`import rlm\nreply = await rlm.host_request('reify.test_session_identity', {'sessionId': ${JSON.stringify(secondId)}})\nreply['sessionId']`,
		);
		expect(secondOwn.status).toBe("ok");
		expect(secondOwn.result).toContain(secondId);
	}, 30_000);
});
