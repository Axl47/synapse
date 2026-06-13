// FILE: 026_ProjectionThreadShellSummary.ts
// Purpose: Adds denormalized shell-summary columns to projection_threads for cheap sidebar snapshots.
// Layer: Persistence migration

import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

import { columnExists } from "./schemaHelpers.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const ensureColumn = (columnName: string, definition: string) =>
    Effect.gen(function* () {
      if (yield* columnExists(sql, "projection_threads", columnName)) {
        return;
      }
      yield* sql.unsafe(`
        ALTER TABLE projection_threads
        ADD COLUMN ${definition}
      `);
    });

  yield* ensureColumn("latest_user_message_at", "latest_user_message_at TEXT");
  yield* ensureColumn(
    "pending_approval_count",
    "pending_approval_count INTEGER NOT NULL DEFAULT 0",
  );
  yield* ensureColumn(
    "pending_user_input_count",
    "pending_user_input_count INTEGER NOT NULL DEFAULT 0",
  );
  yield* ensureColumn(
    "has_actionable_proposed_plan",
    "has_actionable_proposed_plan INTEGER NOT NULL DEFAULT 0",
  );
});
