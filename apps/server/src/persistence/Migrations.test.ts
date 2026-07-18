import { assert, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { migrationEntries, runMigrations } from "./Migrations.ts";
import { MigrationSchemaTooNewError } from "./Errors.ts";
import * as NodeSqliteClient from "./NodeSqliteClient.ts";
import DurableProviderCommandDeliveryMigration from "./Migrations/064_DurableProviderCommandDelivery.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

const trackerRows = (sql: SqlClient.SqlClient) =>
  sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id ASC
  `;

const projectionThreadsColumnNames = (sql: SqlClient.SqlClient) =>
  sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info('projection_threads')
  `.pipe(Effect.map((rows) => rows.map((row) => row.name)));

const tableColumnNames = (sql: SqlClient.SqlClient, tableName: string) =>
  sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info(${tableName})
  `.pipe(Effect.map((rows) => rows.map((row) => row.name)));

const tableIndexNames = (sql: SqlClient.SqlClient, tableName: string) =>
  sql<{ readonly name: string }>`
    SELECT name FROM pragma_index_list(${tableName})
  `.pipe(Effect.map((rows) => rows.map((row) => row.name)));

layer("reconcileMigrationLineage", (it) => {
  // An imported database whose tracker high-water
  // mark is at or beyond Synara's latest migration ID. The migrator's max-ID
  // gate then skips every Synara migration — including the #032 self-heal —
  // and startup crashes on the missing env_mode column.
  it.effect("re-runs skipped migrations when an imported tracker outruns Synara's latest ID", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Bring the schema to the last shared migration.
      yield* runMigrations({ toMigrationInclusive: 16 });

      // Record a foreign lineage from 17 through past Synara's latest ID.
      const latestSynaraId = Math.max(...migrationEntries.map(([id]) => id));
      for (let id = 17; id <= latestSynaraId + 3; id++) {
        yield* sql`
          INSERT INTO effect_sql_migrations (migration_id, name)
          VALUES (${id}, ${`ForeignMigration${id}`})
        `;
      }

      // The foreign lineage added some of the same columns, so the
      // re-run must tolerate columns that already exist.
      yield* sql`ALTER TABLE projection_threads ADD COLUMN archived_at TEXT`;

      const beforeColumns = yield* projectionThreadsColumnNames(sql);
      assert.notInclude(beforeColumns, "env_mode");

      const executed = yield* runMigrations();
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        migrationEntries.map(([id]) => id).filter((id) => id >= 17),
      );

      const afterColumns = yield* projectionThreadsColumnNames(sql);
      assert.include(afterColumns, "env_mode");
      assert.include(afterColumns, "archived_at");

      // The tracker now mirrors the Synara lineage exactly; foreign rows are gone.
      const rows = yield* trackerRows(sql);
      assert.deepStrictEqual(
        rows.map((row) => [row.migration_id, row.name]),
        migrationEntries.map(([id, name]) => [id, name]),
      );
    }),
  );

  it.effect("leaves a healthy tracker alone", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations();
      const executed = yield* runMigrations();
      assert.lengthOf(executed, 0);

      const rows = yield* trackerRows(sql);
      assert.deepStrictEqual(
        rows.map((row) => [row.migration_id, row.name]),
        migrationEntries.map(([id, name]) => [id, name]),
      );
    }),
  );

  it.effect("canonicalizes migration 32 when the preceding lineage is exact", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations();
      yield* sql`
        UPDATE effect_sql_migrations
        SET name = 'PreviousMigration32Name'
        WHERE migration_id = 32
      `;

      const executed = yield* runMigrations();
      assert.lengthOf(executed, 0);
      const rows = yield* trackerRows(sql);
      assert.strictEqual(
        rows.find((row) => row.migration_id === 32)?.name,
        "ReconcileImportedSchemaLineage",
      );
    }),
  );

  it.effect("refuses writable migration startup for a newer Synara schema", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations();
      const futureId = Math.max(...migrationEntries.map(([id]) => id)) + 1;
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (${futureId}, 'FutureSynaraMigration')
      `;

      const rowsBefore = yield* trackerRows(sql);
      const error = yield* Effect.flip(runMigrations());
      assert.instanceOf(error, MigrationSchemaTooNewError);
      assert.strictEqual(error.databaseMigrationId, futureId);
      assert.strictEqual(error.latestSupportedMigrationId, futureId - 1);

      const rows = yield* trackerRows(sql);
      assert.deepStrictEqual(rows, rowsBefore);

      // The suite shares one in-memory database through the layer.
      yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = ${futureId}`;
    }),
  );

  it.effect("normalizes legacy T3 shifted Synara rows before replaying current migrations", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Build the last shared schema in an isolated database, then overlay the
      // legacy ~/.t3 tracker layout observed in user stores. Keeping this
      // fixture below the pending-interaction cutover models a real import and
      // avoids replaying destructive historical migrations over a current DB.
      yield* runMigrations({ toMigrationInclusive: 16 });
      yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id >= 14`;
      for (const [id, name] of [
        [14, "ProjectionThreadContextWindow"],
        [15, "ProjectionThreadsAutorenameCache"],
        [16, "ClearLegacyCodexContextWindow"],
        [17, "ProjectionThreadProposedPlanImplementation"],
        [18, "ProjectionTurnsSourceProposedPlan"],
        [19, "CanonicalizeModelSelections"],
        [20, "ProjectionThreadsArchivedAt"],
        [21, "ProjectionThreadsArchivedAtIndex"],
        [22, "ProjectionSnapshotLookupIndexes"],
        [23, "AuthAccessManagement"],
        [24, "AuthSessionClientMetadata"],
        [25, "AuthSessionLastConnectedAt"],
        [26, "ProjectionThreadShellSummary"],
        [27, "BackfillProjectionThreadShellSummary"],
        [28, "CleanupInvalidProjectionPendingApprovals"],
      ] as const) {
        yield* sql`
          INSERT INTO effect_sql_migrations (migration_id, name)
          VALUES (${id}, ${name})
        `;
      }

      const executed = yield* runMigrations();
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        migrationEntries.map(([id]) => id).filter((id) => id >= 17),
      );

      const rows = yield* trackerRows(sql);
      assert.deepStrictEqual(
        rows.map((row) => [row.migration_id, row.name]),
        migrationEntries.map(([id, name]) => [id, name]),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("refuses to run when the divergence is inside the shared lineage prefix", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations();
      yield* sql`
        UPDATE effect_sql_migrations
        SET name = 'NotAKnownLineage'
        WHERE migration_id = 5
      `;
      const rowsBefore = yield* trackerRows(sql);

      const error = yield* Effect.flip(runMigrations());
      assert.strictEqual(error._tag, "MigrationLineageError");

      // Nothing was deleted on the unrecognized database.
      const rowsAfter = yield* trackerRows(sql);
      assert.deepStrictEqual(rowsAfter, rowsBefore);
    }),
  );

  it.effect("continues when provider instance columns were partially migrated", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 48 });
      const now = new Date().toISOString();
      yield* sql`
        ALTER TABLE projection_thread_sessions
        ADD COLUMN provider_instance_id TEXT
      `;
      yield* sql`
        ALTER TABLE provider_session_runtime
        ADD COLUMN provider_instance_id TEXT
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          env_mode,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'thread-codex-work',
          'project-provider-instance',
          'Work Account Thread',
          ${JSON.stringify({ instanceId: "codex_work", model: "gpt-5.4" })},
          'full-access',
          'default',
          'local',
          ${now},
          ${now},
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_sessions (
          thread_id,
          status,
          provider_name,
          provider_instance_id,
          runtime_mode,
          active_turn_id,
          last_error,
          updated_at
        )
        VALUES
          (
            'thread-codex-work',
            'running',
            'codex',
            NULL,
            'full-access',
            NULL,
            NULL,
            ${now}
          ),
          (
            'thread-no-model-selection',
            'running',
            'codex',
            NULL,
            'full-access',
            NULL,
            NULL,
            ${now}
          )
      `;
      yield* sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          provider_instance_id,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        )
        VALUES
          (
            'thread-codex-work',
            'codex',
            NULL,
            'codex',
            'full-access',
            'running',
            ${now},
            NULL,
            ${JSON.stringify({ modelSelection: { instanceId: "codex_bound", model: "gpt-5.4" } })}
          ),
          (
            'runtime-codex-work',
            'codex',
            NULL,
            'codex',
            'full-access',
            'running',
            ${now},
            NULL,
            ${JSON.stringify({ modelSelection: { instanceId: "codex_work", model: "gpt-5.4" } })}
          ),
          (
            'runtime-no-instance',
            'codex',
            NULL,
            'codex',
            'full-access',
            'running',
            ${now},
            NULL,
            ${JSON.stringify({})}
          )
      `;

      const executed = yield* runMigrations({ toMigrationInclusive: 71 });
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        [
          49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
          68, 69, 70, 71,
        ],
      );

      const projectionSessionColumns = yield* tableColumnNames(sql, "projection_thread_sessions");
      const runtimeColumns = yield* tableColumnNames(sql, "provider_session_runtime");
      assert.include(projectionSessionColumns, "provider_instance_id");
      assert.include(runtimeColumns, "provider_instance_id");

      const projectionSessionIndexes = yield* tableIndexNames(sql, "projection_thread_sessions");
      const runtimeIndexes = yield* tableIndexNames(sql, "provider_session_runtime");
      assert.include(projectionSessionIndexes, "idx_projection_thread_sessions_provider_instance");
      assert.include(runtimeIndexes, "idx_provider_session_runtime_provider_instance");

      const projectionRows = yield* sql<{
        readonly threadId: string;
        readonly providerInstanceId: string | null;
      }>`
        SELECT
          thread_id AS "threadId",
          provider_instance_id AS "providerInstanceId"
        FROM projection_thread_sessions
        WHERE thread_id IN ('thread-codex-work', 'thread-no-model-selection')
        ORDER BY thread_id ASC
      `;
      assert.deepStrictEqual(projectionRows, [
        { threadId: "thread-codex-work", providerInstanceId: "codex_bound" },
        { threadId: "thread-no-model-selection", providerInstanceId: "codex" },
      ]);

      const runtimeRows = yield* sql<{
        readonly threadId: string;
        readonly providerInstanceId: string | null;
      }>`
        SELECT
          thread_id AS "threadId",
          provider_instance_id AS "providerInstanceId"
        FROM provider_session_runtime
        WHERE thread_id IN ('runtime-codex-work', 'runtime-no-instance', 'thread-codex-work')
        ORDER BY thread_id ASC
      `;
      assert.deepStrictEqual(runtimeRows, [
        { threadId: "runtime-codex-work", providerInstanceId: "codex_work" },
        { threadId: "runtime-no-instance", providerInstanceId: "codex" },
        { threadId: "thread-codex-work", providerInstanceId: "codex_bound" },
      ]);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("backfills provider instances without parsing malformed legacy JSON", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const now = new Date().toISOString();

      yield* runMigrations({ toMigrationInclusive: 48 });
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          env_mode,
          created_at,
          updated_at,
          deleted_at
        )
        VALUES (
          'thread-malformed-runtime',
          'project-provider-instance',
          'Malformed Legacy Runtime',
          'not-json',
          'full-access',
          'default',
          'local',
          ${now},
          ${now},
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_sessions (
          thread_id,
          status,
          provider_name,
          runtime_mode,
          active_turn_id,
          last_error,
          updated_at
        )
        VALUES (
          'thread-malformed-runtime',
          'stopped',
          'codex',
          'full-access',
          NULL,
          NULL,
          ${now}
        )
      `;
      yield* sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        )
        VALUES (
          'thread-malformed-runtime',
          'codex',
          'codex',
          'full-access',
          'stopped',
          ${now},
          NULL,
          'not-json'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 71 });

      const [projectionSession] = yield* sql<{
        readonly providerInstanceId: string | null;
      }>`
        SELECT provider_instance_id AS "providerInstanceId"
        FROM projection_thread_sessions
        WHERE thread_id = 'thread-malformed-runtime'
      `;
      const [runtime] = yield* sql<{
        readonly providerInstanceId: string | null;
        readonly runtimePayloadJson: string | null;
      }>`
        SELECT
          provider_instance_id AS "providerInstanceId",
          runtime_payload_json AS "runtimePayloadJson"
        FROM provider_session_runtime
        WHERE thread_id = 'thread-malformed-runtime'
      `;

      assert.deepStrictEqual(projectionSession, { providerInstanceId: "codex" });
      assert.deepStrictEqual(runtime, {
        providerInstanceId: "codex",
        runtimePayloadJson: "not-json",
      });
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});

const providerDeliveryCutoverLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

providerDeliveryCutoverLayer(
  "registered DurableProviderCommandDelivery cutover migration",
  (it) => {
    it.effect("initializes at the event high-water mark when cutover explicitly runs", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 53 });
        const now = new Date().toISOString();

        const inserted = yield* sql<{ readonly sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'evt-before-durable-delivery', 'thread', 'thread-before-durable-delivery', 0,
          'thread.turn-start-requested', ${now}, 'cmd-before-durable-delivery',
          NULL, NULL, 'user', '{"threadId":"thread-before-durable-delivery"}', '{}'
        )
        RETURNING sequence
      `;

        yield* DurableProviderCommandDeliveryMigration;
        const rows = yield* sql<{ readonly lastAckedSequence: number }>`
        SELECT last_acked_sequence AS "lastAckedSequence"
        FROM orchestration_consumer_state
        WHERE consumer_name = 'provider-command-reactor.v1'
      `;
        assert.strictEqual(rows[0]?.lastAckedSequence, inserted[0]?.sequence);

        yield* DurableProviderCommandDeliveryMigration;
        const idempotentRows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM orchestration_consumer_state
        WHERE consumer_name = 'provider-command-reactor.v1'
      `;
        assert.strictEqual(idempotentRows[0]?.count, 1);
      }),
    );
  },
);

const managedAttachmentsFreshLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

managedAttachmentsFreshLayer("managed attachment migration on a fresh database", (it) => {
  it.effect("reserves legacy migration 54 and creates the managed ledger on a fresh database", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const executed = yield* runMigrations();
      assert.deepInclude(executed, [54, "DurableProviderCommandDelivery"]);
      assert.deepInclude(executed, [55, "ManagedAttachments"]);
      assert.deepInclude(executed, [64, "DurableProviderCommandDeliveryCutover"]);
      assert.deepInclude(executed, [65, "DurableQueuedTurnPromotions"]);
      assert.deepInclude(executed, [66, "DurableProviderRuntimeEvents"]);
      assert.deepInclude(executed, [67, "ProviderDeliveryReconciliation"]);

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN ('managed_attachment_blobs', 'managed_attachment_cleanup_jobs')
        ORDER BY name
      `;
      assert.deepStrictEqual(
        tables.map((row) => row.name),
        ["managed_attachment_blobs", "managed_attachment_cleanup_jobs"],
      );

      const providerDeliveryTables = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN ('orchestration_consumer_state', 'orchestration_event_deliveries')
      `;
      assert.strictEqual(providerDeliveryTables[0]?.count, 2);
    }),
  );
});

const managedAttachmentsLegacyLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

managedAttachmentsLegacyLayer("managed attachment migration after private migration 54", (it) => {
  it.effect("keeps a private database that already recorded old migration 54 compatible", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 53 });
      yield* DurableProviderCommandDeliveryMigration;
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (54, 'DurableProviderCommandDelivery')
      `;

      const executed = yield* runMigrations();
      assert.deepStrictEqual(executed, [
        [55, "ManagedAttachments"],
        [56, "CommandReceiptFingerprints"],
        [57, "ThreadScopedProjectionMessageIdentity"],
        [58, "ThreadScopedPendingApprovalIdentity"],
        [59, "ProviderSessionLifecycleGeneration"],
        [60, "PendingApprovalLifecycleGeneration"],
        [61, "PendingApprovalSettlementState"],
        [62, "PendingInteractionSettlementParity"],
        [63, "ProjectionMessageCausalSequence"],
        [64, "DurableProviderCommandDeliveryCutover"],
        [65, "DurableQueuedTurnPromotions"],
        [66, "DurableProviderRuntimeEvents"],
        [67, "ProviderDeliveryReconciliation"],
        [68, "GitHandoffOperations"],
        [69, "ProjectPullRequestPins"],
        [70, "ProjectionThreadSessionProviderInstance"],
        [71, "ProviderSessionRuntimeInstanceId"],
        [72, "ProfileStatsDeletedTurnsProviderInstance"],
        [73, "ProfileStatsDeletedTokensProviderInstance"],
        [74, "ClearAutomationDefinitionProviderOptions"],
        [75, "ClearAutomationRunProviderOptions"],
        [76, "ScrubOrchestrationEventProviderOptions"],
        [77, "ReconcileProjectionSchemaDrift"],
      ]);

      const tracker = yield* trackerRows(sql);
      assert.deepStrictEqual(tracker.slice(-24), [
        { migration_id: 54, name: "DurableProviderCommandDelivery" },
        { migration_id: 55, name: "ManagedAttachments" },
        { migration_id: 56, name: "CommandReceiptFingerprints" },
        { migration_id: 57, name: "ThreadScopedProjectionMessageIdentity" },
        { migration_id: 58, name: "ThreadScopedPendingApprovalIdentity" },
        { migration_id: 59, name: "ProviderSessionLifecycleGeneration" },
        { migration_id: 60, name: "PendingApprovalLifecycleGeneration" },
        { migration_id: 61, name: "PendingApprovalSettlementState" },
        { migration_id: 62, name: "PendingInteractionSettlementParity" },
        { migration_id: 63, name: "ProjectionMessageCausalSequence" },
        { migration_id: 64, name: "DurableProviderCommandDeliveryCutover" },
        { migration_id: 65, name: "DurableQueuedTurnPromotions" },
        { migration_id: 66, name: "DurableProviderRuntimeEvents" },
        { migration_id: 67, name: "ProviderDeliveryReconciliation" },
        { migration_id: 68, name: "GitHandoffOperations" },
        { migration_id: 69, name: "ProjectPullRequestPins" },
        { migration_id: 70, name: "ProjectionThreadSessionProviderInstance" },
        { migration_id: 71, name: "ProviderSessionRuntimeInstanceId" },
        { migration_id: 72, name: "ProfileStatsDeletedTurnsProviderInstance" },
        { migration_id: 73, name: "ProfileStatsDeletedTokensProviderInstance" },
        { migration_id: 74, name: "ClearAutomationDefinitionProviderOptions" },
        { migration_id: 75, name: "ClearAutomationRunProviderOptions" },
        { migration_id: 76, name: "ScrubOrchestrationEventProviderOptions" },
        { migration_id: 77, name: "ReconcileProjectionSchemaDrift" },
      ]);
      const preserved = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM orchestration_consumer_state
      `;
      assert.strictEqual(preserved[0]?.count, 1);
    }),
  );
});

const managedAttachmentsConstraintsLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

managedAttachmentsConstraintsLayer("managed attachment schema constraints", (it) => {
  it.effect(
    "enforces lifecycle, immutable metadata, cleanup ownership, and indexed quota scans",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const now = "2026-07-14T00:00:00.000Z";
        const expiry = "2026-07-15T00:00:00.000Z";

        yield* sql`
        INSERT INTO managed_attachment_blobs (
          attachment_id, owner_thread_id, owner_kind, owner_id, kind,
          original_name, mime_type, reserved_bytes, size_bytes, sha256,
          relative_path, state, staging_expires_at, claim_command_id,
          claim_message_id, claimed_at, delete_reason, delete_requested_at,
          deleted_at, created_at, updated_at
        ) VALUES (
          'att-v2-one', 'Thread/Exact', 'session', 'session-one', 'file',
          'notes.txt', 'text/plain', 1024, NULL, NULL,
          'objects/at/att-v2-one.bin', 'uploading', ${expiry}, NULL,
          NULL, NULL, NULL, NULL, NULL, ${now}, ${now}
        )
      `;

        yield* sql`
        UPDATE managed_attachment_blobs
        SET
          size_bytes = 5,
          sha256 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          state = 'staged',
          updated_at = ${now}
        WHERE attachment_id = 'att-v2-one'
      `;
        yield* sql`
        UPDATE managed_attachment_blobs
        SET
          state = 'claimed',
          claim_command_id = 'command-one',
          claim_message_id = 'message-one',
          claimed_at = ${now},
          updated_at = ${now}
        WHERE attachment_id = 'att-v2-one'
      `;
        yield* sql`
        UPDATE managed_attachment_blobs
        SET
          state = 'deleting',
          delete_reason = 'rollback',
          delete_requested_at = ${now},
          updated_at = ${now}
        WHERE attachment_id = 'att-v2-one'
      `;
        yield* sql`
        INSERT INTO managed_attachment_cleanup_jobs (
          attachment_id, reason, attempt_count, next_attempt_at,
          lease_owner, lease_expires_at, last_error, created_at, updated_at
        ) VALUES (
          'att-v2-one', 'rollback', 0, ${now}, NULL, NULL, NULL, ${now}, ${now}
        )
      `;

        const invalidState = yield* Effect.flip(sql`
        UPDATE managed_attachment_blobs
        SET state = 'staged', updated_at = ${now}
        WHERE attachment_id = 'att-v2-one'
      `);
        assert.isDefined(invalidState);

        const mutatedOwner = yield* Effect.flip(sql`
        UPDATE managed_attachment_blobs
        SET owner_thread_id = 'different-thread'
        WHERE attachment_id = 'att-v2-one'
      `);
        assert.isDefined(mutatedOwner);

        const duplicatePath = yield* Effect.flip(sql`
        INSERT INTO managed_attachment_blobs (
          attachment_id, owner_thread_id, owner_kind, owner_id, kind,
          original_name, mime_type, reserved_bytes, relative_path, state,
          staging_expires_at, created_at, updated_at
        ) VALUES (
          'att-v2-two', 'thread-two', 'session', 'session-two', 'image',
          'image.png', 'image/png', 2048, 'objects/at/att-v2-one.bin',
          'uploading', ${expiry}, ${now}, ${now}
        )
      `);
        assert.isDefined(duplicatePath);

        const missingBlobJob = yield* Effect.flip(sql`
        INSERT INTO managed_attachment_cleanup_jobs (
          attachment_id, reason, attempt_count, next_attempt_at,
          created_at, updated_at
        ) VALUES ('missing', 'gc', 0, ${now}, ${now}, ${now})
      `);
        assert.isDefined(missingBlobJob);

        const quota = yield* sql<{
          readonly reservedBytes: number;
          readonly reservedCount: number;
        }>`
        SELECT
          COALESCE(SUM(reserved_bytes), 0) AS "reservedBytes",
          COUNT(*) AS "reservedCount"
        FROM managed_attachment_blobs
        WHERE state <> 'deleted'
      `;
        assert.deepStrictEqual(quota[0], { reservedBytes: 1024, reservedCount: 1 });

        const blobIndexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_index_list('managed_attachment_blobs')
      `;
        assert.includeMembers(
          blobIndexes.map((row) => row.name),
          [
            "idx_managed_attachment_blobs_state_expiry",
            "idx_managed_attachment_blobs_state_reserved",
            "idx_managed_attachment_blobs_owner_thread",
            "idx_managed_attachment_blobs_owner_principal",
            "idx_managed_attachment_blobs_claim",
          ],
        );
        const cleanupIndexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_index_list('managed_attachment_cleanup_jobs')
      `;
        assert.include(
          cleanupIndexes.map((row) => row.name),
          "idx_managed_attachment_cleanup_jobs_due",
        );
      }),
  );
});

const managedAttachmentsIdempotencyLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

managedAttachmentsIdempotencyLayer("managed attachment migration idempotency", (it) => {
  it.effect("is idempotent after the managed attachment schema is registered", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const executed = yield* runMigrations();
      assert.lengthOf(executed, 0);
    }),
  );
});
