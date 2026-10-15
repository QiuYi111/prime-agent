import { describe, expect, it, vi } from "vitest";
import { createEventBus } from "../src/core/event-bus.js";
import { createExtensionRuntime, loadExtensionFromFactory } from "../src/core/extensions/loader.js";

describe("extension kernel host requests", () => {
	it("stores a namespaced handler on the loaded extension", async () => {
		const handler = vi.fn(async () => ({ ok: true }));
		const extension = await loadExtensionFromFactory(
			(pi) => {
				pi.registerKernelHostRequest({ type: "example.lookup", handler });
			},
			process.cwd(),
			createEventBus(),
			createExtensionRuntime(),
		);

		expect(extension.kernelHostRequests?.get("example.lookup")?.handler).toBe(handler);
	});

	it("rejects invalid and duplicate request types", async () => {
		await expect(
			loadExtensionFromFactory(
				(pi) => {
					pi.registerKernelHostRequest({ type: "not-namespaced", handler: async () => ({}) });
				},
				process.cwd(),
				createEventBus(),
				createExtensionRuntime(),
			),
		).rejects.toThrow("Invalid kernel host request type");

		await expect(
			loadExtensionFromFactory(
				(pi) => {
					pi.registerKernelHostRequest({ type: "example.lookup", handler: async () => ({}) });
					pi.registerKernelHostRequest({ type: "example.lookup", handler: async () => ({}) });
				},
				process.cwd(),
				createEventBus(),
				createExtensionRuntime(),
			),
		).rejects.toThrow("Duplicate kernel host request type");
	});
});
