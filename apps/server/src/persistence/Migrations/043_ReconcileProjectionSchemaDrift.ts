/**
 * Repairs databases whose migration tracker reached the current schema version
 * even though earlier projection columns were never added. This can happen after
 * importing or sharing a DB whose `effect_sql_migrations` IDs came from another
 * migration lineage.
 */
import * as Effect from "effect/Effect";

import ReconcileLegacyT3SchemaImport from "./032_ReconcileLegacyT3SchemaImport.ts";
import ProjectionThreadsSidechatSource from "./033_ProjectionThreadsSidechatSource.ts";
import ProjectionThreadsPinned from "./036_ProjectionThreadsPinned.ts";
import ProjectionThreadsPinnedMessagesNotes from "./040_ProjectionThreadsPinnedMessagesNotes.ts";
import ProjectionProjectsPinned from "./041_ProjectionProjectsPinned.ts";
import ProjectionThreadsMarkers from "./042_ProjectionThreadsMarkers.ts";

export default Effect.gen(function* () {
  yield* ReconcileLegacyT3SchemaImport;
  yield* ProjectionThreadsSidechatSource;
  yield* ProjectionThreadsPinned;
  yield* ProjectionThreadsPinnedMessagesNotes;
  yield* ProjectionProjectsPinned;
  yield* ProjectionThreadsMarkers;
});
