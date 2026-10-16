import type { HostRequestHandlers } from "../kernel/index.js";
import type { Extension, ExtensionContext } from "./types.js";

export function createExtensionKernelHostHandlers(
	extensions: Extension[],
	createContext: (extensionPath: string) => ExtensionContext,
	existingHandlers: HostRequestHandlers = {},
): HostRequestHandlers {
	const handlers: HostRequestHandlers = { ...existingHandlers };
	for (const extension of extensions) {
		for (const registration of extension.kernelHostRequests?.values() ?? []) {
			if (handlers[registration.type]) {
				throw new Error(
					`Kernel host request type ${registration.type} from ${registration.sourceInfo.path} conflicts with another extension`,
				);
			}
			handlers[registration.type] = async (payload) =>
				registration.handler(payload, createContext(registration.sourceInfo.path));
		}
	}
	return handlers;
}
