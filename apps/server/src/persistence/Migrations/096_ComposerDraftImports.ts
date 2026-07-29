import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS composer_draft_imports (
      import_id TEXT PRIMARY KEY CHECK (
        length(import_id) BETWEEN 5 AND 128 AND
        import_id GLOB 'cdi_[a-z0-9]*'
      ),
      idempotency_key TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 512),
      payload_fingerprint TEXT NOT NULL CHECK (
        length(payload_fingerprint) = 64 AND
        payload_fingerprint NOT GLOB '*[^0-9a-f]*'
      ),
      source_json TEXT NOT NULL CHECK (json_valid(source_json)),
      prompt_kind TEXT NOT NULL CHECK (prompt_kind IN ('oneOff', 'reusable')),
      project_id TEXT NOT NULL CHECK (length(project_id) > 0),
      draft_thread_id TEXT NOT NULL UNIQUE CHECK (length(draft_thread_id) > 0),
      title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 255),
      prompt TEXT NOT NULL CHECK (length(prompt) <= 120000),
      prompt_hash TEXT NOT NULL CHECK (
        length(prompt_hash) = 64 AND
        prompt_hash NOT GLOB '*[^0-9a-f]*'
      ),
      presets_json TEXT NOT NULL CHECK (json_valid(presets_json)),
      status TEXT NOT NULL CHECK (
        status IN ('uploading', 'ready', 'claiming', 'completed', 'failed', 'cancelled', 'expired')
      ),
      lease_id TEXT,
      lease_expires_at TEXT,
      failure_message TEXT CHECK (failure_message IS NULL OR length(failure_message) <= 500),
      expires_at TEXT NOT NULL,
      completed_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      CHECK (
        (status = 'claiming' AND lease_id IS NOT NULL AND lease_expires_at IS NOT NULL) OR
        (status <> 'claiming' AND lease_id IS NULL AND lease_expires_at IS NULL)
      ),
      CHECK (
        (status = 'completed' AND completed_at IS NOT NULL) OR
        (status <> 'completed' AND completed_at IS NULL)
      )
    )
  `;

  yield* sql`
    CREATE TABLE IF NOT EXISTS composer_draft_import_attachments (
      import_id TEXT NOT NULL,
      attachment_id TEXT NOT NULL CHECK (
        length(attachment_id) BETWEEN 1 AND 128 AND
        attachment_id NOT GLOB '*[^A-Za-z0-9_-]*'
      ),
      managed_attachment_id TEXT UNIQUE,
      kind TEXT NOT NULL CHECK (kind IN ('image', 'file')),
      original_name TEXT NOT NULL CHECK (length(original_name) BETWEEN 1 AND 255),
      mime_type TEXT NOT NULL CHECK (length(mime_type) BETWEEN 1 AND 100),
      expected_bytes INTEGER NOT NULL CHECK (expected_bytes >= 0),
      attachment_order INTEGER NOT NULL CHECK (attachment_order >= 0),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (import_id, attachment_id),
      UNIQUE (import_id, attachment_order),
      FOREIGN KEY (import_id)
        REFERENCES composer_draft_imports(import_id)
        ON DELETE CASCADE,
      FOREIGN KEY (managed_attachment_id)
        REFERENCES managed_attachment_blobs(attachment_id)
        ON DELETE RESTRICT
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_composer_draft_imports_status_expiry
    ON composer_draft_imports(status, expires_at, import_id)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_composer_draft_imports_claim_lease
    ON composer_draft_imports(status, lease_expires_at, import_id)
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_composer_draft_import_attachments_storage
    ON composer_draft_import_attachments(managed_attachment_id)
  `;

  yield* sql`
    CREATE TRIGGER IF NOT EXISTS trg_composer_draft_imports_immutable_payload
    BEFORE UPDATE OF
      idempotency_key,
      payload_fingerprint,
      source_json,
      prompt_kind,
      project_id,
      draft_thread_id,
      title,
      prompt,
      prompt_hash,
      presets_json,
      created_at
    ON composer_draft_imports
    BEGIN
      SELECT RAISE(ABORT, 'composer draft import payload is immutable');
    END
  `;

  yield* sql`
    CREATE TRIGGER IF NOT EXISTS trg_composer_draft_imports_state_transition
    BEFORE UPDATE OF status
    ON composer_draft_imports
    WHEN NOT (
      NEW.status = OLD.status OR
      (OLD.status = 'uploading' AND NEW.status IN ('ready', 'failed', 'cancelled', 'expired')) OR
      (OLD.status = 'ready' AND NEW.status IN ('claiming', 'failed', 'cancelled', 'expired')) OR
      (OLD.status = 'claiming' AND NEW.status IN ('ready', 'completed', 'failed', 'cancelled', 'expired'))
    )
    BEGIN
      SELECT RAISE(ABORT, 'invalid composer draft import state transition');
    END
  `;
});

