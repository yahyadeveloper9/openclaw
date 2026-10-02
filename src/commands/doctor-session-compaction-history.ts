import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { asOptionalRecord, isRecord } from "@openclaw/normalization-core/record-coerce";
import { sql } from "kysely";
import { SessionStoreMigrationRequiredError } from "../config/sessions/migration-required.js";
import { getSessionKysely } from "../config/sessions/session-accessor.sqlite-scope.js";
import { updateSqliteTranscriptEventJsonInTransaction } from "../config/sessions/session-accessor.sqlite-transcript-store.js";
import { assertSessionTranscriptHot } from "../config/sessions/session-cold-storage-state.js";
import { assertCanonicalRetainedHistoryReferences } from "../config/sessions/session-retained-history.js";
import { transcriptEventJsonSql } from "../config/sessions/transcript-payload.js";
import type { RetainedHistoryReferences } from "../config/sessions/types.js";
import { resolveRealpathOrAbsolute } from "../infra/boundary-path.js";
import { executeSqliteQuerySync } from "../infra/kysely-sync.js";
import type { OpenClawAgentDatabase } from "../state/openclaw-agent-db.js";

type LegacyCompactionHistory = {
  sessionId?: string;
  preCompaction: { sessionId?: string; sessionFile?: string };
  postCompaction: { sessionId?: string; sessionFile?: string; entryId?: string };
  tokensBefore?: number;
  tokensAfter?: number;
};

export type LegacyCompactionEventFact = {
  sessionId: string;
  entryId: string;
  tokensBefore?: number;
  tokensAfter?: number;
};

function patchCompactionEvent(
  database: DatabaseSync,
  eventJson: string,
  event: Record<string, unknown>,
  fact: LegacyCompactionEventFact,
): string {
  let patched = sql.val(eventJson);
  let changed = false;
  for (const key of ["tokensBefore", "tokensAfter"] as const) {
    const value = fact[key];
    if (value !== undefined && event[key] !== value) {
      // Preserve opaque numeric tokens and unrelated payload fields during metric repair.
      patched = sql<string>`json_set(${patched}, ${`$.${key}`}, ${value})`;
      changed = true;
    }
  }
  return changed
    ? executeSqliteQuerySync(
        database,
        getSessionKysely(database).selectNoFrom(patched.as("event_json")),
      ).rows[0]!.event_json
    : eventJson;
}

/** Doctor import and verification compare the same prepared metric projection. */
export function createLegacyCompactionTranscriptTransform(
  eventFacts: readonly LegacyCompactionEventFact[],
): (database: DatabaseSync, sessionId: string, eventJson: string) => string {
  const sessions = new Map<
    string,
    Map<string, { fact: LegacyCompactionEventFact; conflicting: boolean }>
  >();
  for (const fact of eventFacts) {
    const markers =
      sessions.get(fact.sessionId) ??
      new Map<string, { fact: LegacyCompactionEventFact; conflicting: boolean }>();
    const previous = markers.get(fact.entryId);
    markers.set(fact.entryId, {
      fact: {
        ...fact,
        tokensBefore: fact.tokensBefore ?? previous?.fact.tokensBefore,
        tokensAfter: fact.tokensAfter ?? previous?.fact.tokensAfter,
      },
      conflicting: Boolean(
        previous &&
        (previous.conflicting ||
          (previous.fact.tokensBefore !== undefined &&
            fact.tokensBefore !== undefined &&
            previous.fact.tokensBefore !== fact.tokensBefore) ||
          (previous.fact.tokensAfter !== undefined &&
            fact.tokensAfter !== undefined &&
            previous.fact.tokensAfter !== fact.tokensAfter)),
      ),
    });
    sessions.set(fact.sessionId, markers);
  }
  return (database, sessionId, eventJson) => {
    const markers = sessions.get(sessionId);
    if (!markers) {
      return eventJson;
    }
    const event: unknown = JSON.parse(eventJson);
    if (!isRecord(event) || event.type !== "compaction" || typeof event.id !== "string") {
      return eventJson;
    }
    const selected = markers.get(event.id);
    if (selected?.conflicting) {
      throw new SessionStoreMigrationRequiredError(
        `Conflicting legacy compaction facts for ${sessionId}:${event.id}`,
      );
    }
    return selected ? patchCompactionEvent(database, eventJson, event, selected.fact) : eventJson;
  };
}

function readLegacyCompactionHistory(entry: Record<string, unknown>): LegacyCompactionHistory[] {
  const checkpoints = entry.compactionCheckpoints;
  if (checkpoints == null) {
    return [];
  }
  if (!Array.isArray(checkpoints)) {
    throw new SessionStoreMigrationRequiredError("Invalid legacy compaction history references");
  }
  return checkpoints.map((checkpoint): LegacyCompactionHistory => {
    const record = asOptionalRecord(checkpoint);
    const pre = asOptionalRecord(record?.preCompaction);
    const post = asOptionalRecord(record?.postCompaction);
    if (!record || !pre || !post) {
      throw new SessionStoreMigrationRequiredError("Invalid legacy compaction history references");
    }
    const string = (value: unknown): string | undefined => {
      if (value == null) {
        return undefined;
      }
      if (typeof value !== "string") {
        throw new SessionStoreMigrationRequiredError("Invalid legacy compaction history reference");
      }
      return value;
    };
    const number = (value: unknown): number | undefined =>
      typeof value === "number" && Number.isFinite(value) ? value : undefined;
    return {
      sessionId: string(record.sessionId),
      preCompaction: { sessionId: string(pre.sessionId), sessionFile: string(pre.sessionFile) },
      postCompaction: {
        sessionId: string(post.sessionId),
        sessionFile: string(post.sessionFile),
        entryId: string(post.entryId),
      },
      tokensBefore: number(record.tokensBefore),
      tokensAfter: number(record.tokensAfter),
    };
  });
}

function retainedReferences(
  entry: Record<string, unknown>,
  checkpoints: readonly LegacyCompactionHistory[],
): RetainedHistoryReferences {
  const current = entry.retainedHistoryReferences;
  assertCanonicalRetainedHistoryReferences(current);
  const sessionIds = new Set(current?.sessionIds);
  const artifactPaths = new Set(current?.artifactPaths);
  for (const checkpoint of checkpoints) {
    for (const id of [
      checkpoint.sessionId,
      checkpoint.preCompaction.sessionId,
      checkpoint.postCompaction.sessionId,
    ]) {
      if (id?.trim()) {
        sessionIds.add(id.trim());
      }
    }
    for (const file of [
      checkpoint.preCompaction.sessionFile,
      checkpoint.postCompaction.sessionFile,
    ]) {
      const trimmed = file?.trim();
      if (!trimmed) {
        continue;
      }
      if (!path.isAbsolute(trimmed) || trimmed.includes("\0")) {
        throw new SessionStoreMigrationRequiredError(
          "Legacy compaction artifact needs an absolute path before migration",
        );
      }
      // Match the disk-budget owner's existing physical-path comparison.
      artifactPaths.add(resolveRealpathOrAbsolute(trimmed));
    }
  }
  const references = { sessionIds: [...sessionIds], artifactPaths: [...artifactPaths] };
  assertCanonicalRetainedHistoryReferences(references);
  return references;
}

function prepareEventFacts(
  entry: Record<string, unknown>,
  checkpoints: readonly LegacyCompactionHistory[],
): LegacyCompactionEventFact[] {
  // Marker ids are scoped to a transcript; the final checkpoint wins within that pair.
  const byMarker = new Map<
    string,
    { sessionId: string; entryId: string; checkpoint: LegacyCompactionHistory }
  >();
  for (const checkpoint of checkpoints) {
    const entryId = checkpoint.postCompaction.entryId;
    if (entryId) {
      const sessionId =
        checkpoint.postCompaction.sessionId?.trim() ||
        checkpoint.sessionId?.trim() ||
        (typeof entry.sessionId === "string" ? entry.sessionId.trim() : undefined);
      if (!sessionId) {
        continue;
      }
      const identity = JSON.stringify([sessionId, entryId]);
      byMarker.set(identity, {
        sessionId,
        entryId,
        checkpoint,
      });
    }
  }
  const eventFacts: LegacyCompactionEventFact[] = [];
  for (const { sessionId, entryId, checkpoint } of byMarker.values()) {
    if (checkpoint.tokensBefore === undefined && checkpoint.tokensAfter === undefined) {
      continue;
    }
    eventFacts.push({
      sessionId,
      entryId,
      ...(checkpoint.tokensBefore !== undefined ? { tokensBefore: checkpoint.tokensBefore } : {}),
      ...(checkpoint.tokensAfter !== undefined ? { tokensAfter: checkpoint.tokensAfter } : {}),
    });
  }
  return eventFacts;
}

/** Apply prepared facts inside the transaction that publishes their canonical session entry. */
export function applyLegacyCompactionEventFacts(
  database: OpenClawAgentDatabase,
  eventFacts: readonly LegacyCompactionEventFact[],
  transform = createLegacyCompactionTranscriptTransform(eventFacts),
): void {
  const updatesBySession = new Map<string, Array<{ seq: number; eventJson: string }>>();
  const db = getSessionKysely(database.db);
  for (const fact of eventFacts) {
    const { sessionId, entryId } = fact;
    assertSessionTranscriptHot(database.db, sessionId);
    const payload = transcriptEventJsonSql(database.db);
    const rows = executeSqliteQuerySync(
      database.db,
      db
        .selectFrom("transcript_events")
        .select(["seq", payload.as("event_json")])
        .where("session_id", "=", sessionId)
        // Doctor matches the persisted marker, including rows with an unbuilt identity index.
        .where(sql<string>`json_extract(${payload}, '$.id')`, "=", entryId)
        .where(sql<string>`json_extract(${payload}, '$.type')`, "=", "compaction")
        .limit(2),
    ).rows;
    const row = rows[0];
    if (!row) {
      continue;
    }
    const event: unknown = JSON.parse(row.event_json);
    if (
      rows.length !== 1 ||
      !isRecord(event) ||
      event.type !== "compaction" ||
      event.id !== entryId
    ) {
      throw new SessionStoreMigrationRequiredError(
        `Conflicting compaction marker ${sessionId}:${entryId}`,
      );
    }
    const eventJson = transform(database.db, sessionId, row.event_json);
    if (eventJson !== row.event_json) {
      const updates = updatesBySession.get(sessionId) ?? [];
      updates.push({ seq: row.seq, eventJson });
      updatesBySession.set(sessionId, updates);
    }
  }
  for (const [sessionId, updates] of updatesBySession) {
    updateSqliteTranscriptEventJsonInTransaction(database, sessionId, updates);
  }
}

/** Prepare canonical retention/metric facts without writes for the importing transaction. */
export function prepareLegacySessionCompactionHistory(entry: Record<string, unknown>): {
  entry: Record<string, unknown>;
  eventFacts: LegacyCompactionEventFact[];
} {
  if (!Object.hasOwn(entry, "compactionCheckpoints")) {
    return { entry, eventFacts: [] };
  }
  const checkpoints = readLegacyCompactionHistory(entry);
  const references = retainedReferences(entry, checkpoints);
  const eventFacts = prepareEventFacts(entry, checkpoints);
  // Checkpoint ids, redundant owner/trigger/time/version fields, leaf pointers, and duplicate
  // summary/boundary metadata belonged to the retired feature; the original backup retains them.
  const { compactionCheckpoints: _retired, ...next } = entry;
  if (references.sessionIds.length > 0 || references.artifactPaths.length > 0) {
    next.retainedHistoryReferences = references;
  }
  return { entry: next, eventFacts };
}
