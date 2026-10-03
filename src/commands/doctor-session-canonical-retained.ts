import { publishSessionEntryCacheInvalidation } from "../config/sessions/session-accessor.sqlite-entry-cache.js";
import { assertCanonicalSessionKeyWrite } from "../config/sessions/session-canonical-key.js";
import { certifyCanonicalSessionValidationRow } from "../config/sessions/session-canonical-validation.js";
import { splitSessionEntrySnapshots } from "../config/sessions/session-entry-snapshots.js";
import type { SessionEntry } from "../config/sessions/types.js";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../infra/kysely-sync.js";
import type { DB } from "../state/openclaw-agent-db.generated.js";
import type { OpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import type { CanonicalSessionCandidate } from "./doctor-session-canonical-candidates.js";

type RetainedSessionDatabase = Pick<
  DB,
  "session_nodes" | "session_windows" | "session_entry_snapshots"
>;

export function assertRetainedSnapshotsMatchCanonicalEntry(
  source: OpenClawAgentDatabase,
  candidate: Extract<CanonicalSessionCandidate, { kind: "retained" }>,
  entry: SessionEntry,
): void {
  const snapshots = executeSqliteQuerySync(
    source.db,
    getNodeSqliteKysely<RetainedSessionDatabase>(source.db)
      .selectFrom("session_entry_snapshots")
      .select(["field", "value_json"])
      .where("session_key", "=", candidate.sessionKey),
  ).rows;
  const selected = splitSessionEntrySnapshots(entry).snapshots;
  for (const snapshot of snapshots) {
    if (
      selected.find((value) => value.field === snapshot.field)?.valueJson !== snapshot.value_json
    ) {
      throw new Error(
        `Retained history ${candidate.sessionKey} conflicts with ${candidate.canonicalKey} in ${snapshot.field}; preserve both owners and resolve their snapshots before retrying Doctor.`,
      );
    }
  }
}

/** Prepare the retained owner inside the canonical repair's existing source and destination custody. */
export function prepareCanonicalRetainedOwner(params: {
  source: OpenClawAgentDatabase;
  destination: OpenClawAgentDatabase;
  candidate: Extract<CanonicalSessionCandidate, { kind: "retained" }>;
  canonicalKey: string;
}): { created: boolean } {
  const { source, destination, candidate, canonicalKey } = params;
  // The planner binds the logical agent; a shared store can have a different physical owner.
  assertCanonicalSessionKeyWrite(canonicalKey);
  const sourceDb = getNodeSqliteKysely<RetainedSessionDatabase>(source.db);
  const sourceRow = executeSqliteQueryTakeFirstSync(
    source.db,
    sourceDb
      .selectFrom("session_nodes")
      .selectAll()
      .where("session_key", "=", candidate.sessionKey),
  );
  const sourceWindow = executeSqliteQueryTakeFirstSync(
    source.db,
    sourceDb
      .selectFrom("session_windows")
      .select("session_key")
      .where("session_id", "=", candidate.sessionId),
  );
  if (
    !sourceRow ||
    sourceRow.entry_json !== candidate.rawEntryJson ||
    sourceRow.entry_valid !== -1 ||
    sourceRow.current_session_id !== candidate.sessionId ||
    sourceRow.updated_at !== candidate.updatedAt ||
    sourceRow.snapshot_revision !== candidate.rawSnapshotRevision ||
    sourceWindow?.session_key !== candidate.sessionKey
  ) {
    throw new Error(`Retained history owner changed for ${candidate.sessionKey}; rerun Doctor.`);
  }
  const sourceSnapshots = executeSqliteQuerySync(
    source.db,
    sourceDb
      .selectFrom("session_entry_snapshots")
      .selectAll()
      .where("session_key", "=", candidate.sessionKey)
      .orderBy("field"),
  ).rows;
  const destinationDb = getNodeSqliteKysely<RetainedSessionDatabase>(destination.db);
  const destinationRow = executeSqliteQueryTakeFirstSync(
    destination.db,
    destinationDb
      .selectFrom("session_nodes")
      .select("session_key")
      .where("session_key", "=", canonicalKey),
  );
  if (destinationRow) {
    const destinationSnapshots = executeSqliteQuerySync(
      destination.db,
      destinationDb
        .selectFrom("session_entry_snapshots")
        .select(["field", "value_json"])
        .where("session_key", "=", canonicalKey),
    ).rows;
    for (const snapshot of sourceSnapshots) {
      const existing = destinationSnapshots.find((value) => value.field === snapshot.field);
      if (existing?.value_json !== snapshot.value_json) {
        throw new Error(
          `Retained history ${candidate.sessionKey} conflicts with ${canonicalKey} in ${snapshot.field}; preserve both owners and resolve their snapshots before retrying Doctor.`,
        );
      }
    }
    return { created: false };
  }
  executeSqliteQuerySync(
    destination.db,
    destinationDb.insertInto("session_nodes").values({ ...sourceRow, session_key: canonicalKey }),
  );
  for (const snapshot of sourceSnapshots) {
    executeSqliteQuerySync(
      destination.db,
      destinationDb
        .insertInto("session_entry_snapshots")
        .values({ ...snapshot, session_key: canonicalKey }),
    );
  }
  return { created: true };
}

/** Certify only after the canonical owner has received its retained windows. */
export function finishCanonicalRetainedOwner(
  database: OpenClawAgentDatabase,
  canonicalKey: string,
  created: boolean,
): void {
  if (created) {
    executeSqliteQuerySync(
      database.db,
      getNodeSqliteKysely<RetainedSessionDatabase>(database.db)
        .updateTable("session_nodes")
        .set({ entry_valid: -1 })
        .where("session_key", "=", canonicalKey),
    );
  }
  certifyCanonicalSessionValidationRow(database, canonicalKey);
  publishSessionEntryCacheInvalidation(database, { sessionKey: canonicalKey });
}
