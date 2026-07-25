// FILE: threadDisplayProvider.ts
// Purpose: Resolve the provider shown for a thread in UI surfaces (chips, pickers).
// Layer: Web display helper
// Exports: resolveThreadDisplayProvider

import type { ModelSelection, ProviderKind } from "@synara/contracts";
import { inferLegacyProviderKindFromModelSelection } from "@synara/shared/providerInstances";

/** The live session's provider wins over the configured model selection. */
export function resolveThreadDisplayProvider(thread: {
  readonly session?: { readonly provider: ProviderKind } | null;
  readonly modelSelection: ModelSelection;
}): ProviderKind {
  return thread.session?.provider ?? inferLegacyProviderKindFromModelSelection(thread.modelSelection);
}
