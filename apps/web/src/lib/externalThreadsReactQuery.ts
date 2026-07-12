import type { OrchestrationListExternalThreadsResult } from "@synara/contracts";
import { queryOptions } from "@tanstack/react-query";

import { ensureNativeApi } from "../nativeApi";

export const externalThreadQueryKeys = {
  all: ["orchestration", "external-threads"] as const,
  list: () => [...externalThreadQueryKeys.all, "list"] as const,
};

export function externalThreadsQueryOptions(enabled = true) {
  return queryOptions<OrchestrationListExternalThreadsResult>({
    queryKey: externalThreadQueryKeys.list(),
    queryFn: () => ensureNativeApi().orchestration.listExternalThreads({}),
    enabled,
    staleTime: 30_000,
    retry: 1,
    refetchOnWindowFocus: true,
  });
}
