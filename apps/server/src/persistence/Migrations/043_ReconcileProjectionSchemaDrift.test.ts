import { assert, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

const columnNames = (sql: SqlClient.SqlClient, tableName: string) =>
  sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info(${tableName})
  `.pipe(Effect.map((rows) => new Set(rows.map((row) => row.name))));

layer("044_ReconcileProjectionSchemaDrift", (it) => {
  it.effect("heals projection columns when the tracker already claims 17-42 ran", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 16 });

      for (let migrationId = 17; migrationId <= 42; migrationId += 1) {
        yield* sql`
          INSERT INTO effect_sql_migrations (migration_id, name)
          VALUES (${migrationId}, ${`PreSynara_${migrationId}`})
        `;
      }

      const beforeThreadsColumns = yield* columnNames(sql, "projection_threads");
      assert.isFalse(beforeThreadsColumns.has("env_mode"));
      assert.isFalse(beforeThreadsColumns.has("thread_markers_json"));

      yield* runMigrations();

      const afterThreadsColumns = yield* columnNames(sql, "projection_threads");
      const afterMessagesColumns = yield* columnNames(sql, "projection_thread_messages");
      const afterProjectsColumns = yield* columnNames(sql, "projection_projects");

      for (const column of [
        "handoff_json",
        "env_mode",
        "fork_source_thread_id",
        "associated_worktree_path",
        "associated_worktree_branch",
        "associated_worktree_ref",
        "archived_at",
        "parent_thread_id",
        "subagent_agent_id",
        "subagent_nickname",
        "subagent_role",
        "latest_user_message_at",
        "pending_approval_count",
        "pending_user_input_count",
        "has_actionable_proposed_plan",
        "last_known_pr_json",
        "create_branch_flow_completed",
        "sidechat_source_thread_id",
        "is_pinned",
        "pinned_messages_json",
        "notes",
        "thread_markers_json",
      ]) {
        assert.isTrue(afterThreadsColumns.has(column), `projection_threads.${column}`);
      }

      for (const column of ["source", "skills_json", "mentions_json", "dispatch_mode"]) {
        assert.isTrue(afterMessagesColumns.has(column), `projection_thread_messages.${column}`);
      }

      for (const column of ["kind", "is_pinned"]) {
        assert.isTrue(afterProjectsColumns.has(column), `projection_projects.${column}`);
      }
    }),
  );
});
