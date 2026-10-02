import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { expectDefined } from "@openclaw/normalization-core";
import { expect, it } from "vitest";
import { getSessionKysely } from "../config/sessions/session-accessor.sqlite-scope.js";
import { readSessionColdTranscript } from "../config/sessions/session-cold-storage-state.js";
import { runSessionColdStorageMaintenance } from "../config/sessions/session-cold-storage.js";
import { transcriptEventJsonSql } from "../config/sessions/transcript-payload.js";
import { executeSqliteQuerySync } from "../infra/kysely-sync.js";
import { autoMigrateLegacyState } from "../infra/state-migrations.doctor.js";
import { EMPTY_LEGACY_SESSION_SURFACES } from "../plugins/legacy-session-surfaces.types.js";
import { openOpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import {
  withOpenClawTestState,
  type OpenClawTestState,
} from "../test-utils/openclaw-test-state.js";
import { repairLegacySessionEntryStates } from "./doctor-session-delivery-state.js";
import { withDoctorSqliteMaintenanceLock } from "./doctor-sqlite-maintenance-lock.js";

function seedHistory(
  state: OpenClawTestState,
  key: string,
  fields: Record<string, unknown>,
  markerType = "compaction",
  markerFields: Record<string, unknown> = {},
) {
  const database = openOpenClawAgentDatabase({ agentId: "main", env: state.env });
  const sessionKey = `agent:main:${key}`;
  const sessionId = `session-${key}`;
  const raw = JSON.stringify({ sessionId, updatedAt: 42, opaqueCount: 0, ...fields }).replace(
    '"opaqueCount":0',
    '"opaqueCount":9007199254740993',
  );
  // Seed the historical physical format; the public Doctor path owns its conversion.
  database.db
    .prepare(
      "INSERT INTO session_nodes (session_key, current_session_id, entry_json, updated_at, entry_valid) VALUES (?, ?, ?, 42, 1)",
    )
    .run(sessionKey, sessionId, raw);
  database.db
    .prepare(
      "INSERT INTO session_windows (session_id, session_key, created_at, updated_at, transcript_updated_at) VALUES (?, ?, 42, 42, 42)",
    )
    .run(sessionId, sessionKey);
  const eventJson = JSON.stringify({
    type: markerType,
    id: "compact",
    parentId: "user",
    timestamp: "2026-07-01T00:00:00.000Z",
    summary: "preserved summary",
    firstKeptEntryId: "user",
    tokensBefore: 500,
    opaqueCount: 0,
    ...markerFields,
  }).replace('"opaqueCount":0', '"opaqueCount":9007199254740993');
  const insertEvent = database.db.prepare(
    "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?, ?, ?, 42)",
  );
  insertEvent.run(sessionId, 1, JSON.stringify({ type: "session", id: sessionId, version: 3 }));
  insertEvent.run(
    sessionId,
    2,
    JSON.stringify({
      type: "message",
      id: "user",
      parentId: null,
      timestamp: "2026-07-01T00:00:00.000Z",
      message: { role: "user", content: "retained question", timestamp: 42 },
    }),
  );
  insertEvent.run(sessionId, 3, eventJson);
  database.db
    .prepare(
      "INSERT INTO transcript_rewrite_watermarks (session_id, generation, updated_at) VALUES (?, 'original-generation', 42)",
    )
    .run(sessionId);
  const read = () => {
    const current = openOpenClawAgentDatabase({ agentId: "main", env: state.env });
    const row = current.db
      .prepare("SELECT entry_json, updated_at FROM session_nodes WHERE session_key = ?")
      .get(sessionKey);
    const event = executeSqliteQuerySync(
      current.db,
      getSessionKysely(current.db)
        .selectFrom("transcript_events")
        .select(["seq", "created_at", transcriptEventJsonSql(current.db).as("event_json")])
        .where("session_id", "=", sessionId)
        .where("seq", "=", 3),
    ).rows[0];
    const window = current.db
      .prepare("SELECT transcript_updated_at FROM session_windows WHERE session_id = ?")
      .get(sessionId);
    return {
      raw: row?.entry_json,
      updatedAt: row?.updated_at,
      event,
      transcriptUpdatedAt: window?.transcript_updated_at,
    };
  };
  return { databasePath: database.path, sessionId, sessionKey, raw, eventJson, read };
}

function checkpoint(sessionId: string, tokensBefore = 90) {
  return {
    sessionId,
    preCompaction: { sessionId: " previous " },
    postCompaction: { sessionId, entryId: "compact" },
    tokensBefore,
    tokensAfter: 40,
    summary: "preserved summary",
    firstKeptEntryId: "user",
  };
}

async function repair(state: OpenClawTestState) {
  return await withDoctorSqliteMaintenanceLock({
    env: state.env,
    operation: "compaction history migration test",
    run: (authority) =>
      repairLegacySessionEntryStates({ apply: true, cfg: {}, env: state.env, authority }),
  });
}

it("backs up and atomically moves checkpoint facts into canonical retention and transcript metrics", async () => {
  await withOpenClawTestState(
    { label: "doctor-compaction-history", scenario: "minimal" },
    async (state) => {
      const artifact = state.path("snapshot.jsonl");
      const latest = { ...checkpoint("session-history"), tokensAfter: undefined };
      const summary = "\n    canonical summary\n";
      const earlier = seedHistory(
        state,
        "earlier",
        {
          compactionCheckpoints: [
            {
              ...checkpoint("session-history"),
              tokensBefore: undefined,
              tokensAfter: 7,
            },
          ],
        },
        "compaction",
        { summary: "earlier canonical summary" },
      );
      const fixture = seedHistory(
        state,
        "history",
        {
          compactionCheckpoints: [
            { ...checkpoint(earlier.sessionId, 240), summary: "earlier preserved summary" },
            checkpoint("session-history", 120),
            {
              ...latest,
              summary: "unused checkpoint summary",
              firstKeptEntryId: "unused-checkpoint-boundary",
              preCompaction: { ...latest.preCompaction, sessionFile: ` ${artifact} ` },
            },
          ],
          retainedHistoryReferences: {
            sessionIds: ["already-retained"],
            artifactPaths: [artifact],
          },
          pendingFinalDelivery: true,
          pendingFinalDeliveryText: "preserved reply",
          pluginExtensions: { example: { saved: { text: "unchanged" } } },
        },
        "compaction",
        { summary, tokensAfter: 7 },
      );
      expect(
        await repairLegacySessionEntryStates({ apply: false, cfg: {}, env: state.env }),
      ).toMatchObject({ found: 2, repaired: 0 });
      expect(fixture.read().raw).toBe(fixture.raw);
      expect(await repair(state)).toMatchObject({ found: 2, repaired: 2 });
      const after = fixture.read();
      expect(JSON.parse(String(after.raw))).toMatchObject({
        sessionId: fixture.sessionId,
        updatedAt: 42,
        retainedHistoryReferences: {
          sessionIds: ["already-retained", earlier.sessionId, "previous", "session-history"],
          artifactPaths: [artifact],
        },
        pendingFinalDelivery: { kind: "replayable", text: "preserved reply", createdAt: 42 },
        pluginExtensions: { example: { saved: { text: "unchanged" } } },
      });
      expect(after.raw).not.toContain("compactionCheckpoints");
      expect(after.raw).toContain('"opaqueCount":9007199254740993');
      expect(after.updatedAt).toBe(42);
      expect(after.transcriptUpdatedAt).toBe(42);
      const event = expectDefined(after.event, "repaired compaction event");
      expect(JSON.parse(event.event_json)).toMatchObject({
        type: "compaction",
        id: "compact",
        summary,
        firstKeptEntryId: "user",
        tokensBefore: 90,
        tokensAfter: 7,
      });
      expect(event).toMatchObject({ seq: 3, created_at: 42 });
      expect(event.event_json).toContain('"opaqueCount":9007199254740993');
      const earlierEvent = expectDefined(earlier.read().event, "earlier generation marker");
      expect(JSON.parse(earlierEvent.event_json)).toMatchObject({
        id: "compact",
        summary: "earlier canonical summary",
        firstKeptEntryId: "user",
        tokensBefore: 240,
        tokensAfter: 40,
      });
      const backupName = expectDefined(
        fs
          .readdirSync(path.dirname(fixture.databasePath))
          .find((name) =>
            name.startsWith(`${path.basename(fixture.databasePath)}.pre-startup-migration-`),
          ),
        "Doctor database backup",
      );
      using backup = new DatabaseSync(path.join(path.dirname(fixture.databasePath), backupName), {
        readOnly: true,
      });
      expect(
        backup
          .prepare("SELECT entry_json FROM session_nodes WHERE session_key = ?")
          .get(fixture.sessionKey)?.entry_json,
      ).toBe(fixture.raw);
      expect(
        backup
          .prepare("SELECT event_json FROM transcript_events WHERE session_id = ? AND seq = 3")
          .get(fixture.sessionId)?.event_json,
      ).toBe(fixture.eventJson);
      expect(
        await repairLegacySessionEntryStates({ apply: true, cfg: {}, env: state.env }),
      ).toEqual({ found: 0, repaired: 0, scannedStores: 1 });
    },
  );
});

it("restores cold compaction metrics after backup without changing marker text", async () => {
  await withOpenClawTestState(
    { label: "doctor-compaction-cold", scenario: "minimal" },
    async (state) => {
      const summary = "  preserved summary\n";
      const fixture = seedHistory(state, "cold", {}, "compaction", { summary });
      expect(
        await runSessionColdStorageMaintenance({
          config: {
            agents: { ownership: "explicit", entries: { main: {} } },
            session: {
              store: fixture.databasePath,
              maintenance: { coldStorage: { enabled: true, afterDays: 30 } },
            },
          },
        }),
      ).toEqual({ archivedTranscripts: 1, externalizedTranscripts: 0 });
      const database = openOpenClawAgentDatabase({ agentId: "main", env: state.env });
      expect(readSessionColdTranscript(database.db, fixture.sessionId)).toBeDefined();
      const legacy = JSON.stringify({
        sessionId: fixture.sessionId,
        updatedAt: 42,
        compactionCheckpoints: [
          {
            ...checkpoint(fixture.sessionId),
            sessionId: fixture.sessionId,
            postCompaction: { sessionId: fixture.sessionId, entryId: "compact" },
            summary: "unused cold checkpoint summary",
            firstKeptEntryId: "unused-cold-boundary",
            preCompaction: { sessionId: fixture.sessionId },
          },
        ],
      });
      database.db
        .prepare("UPDATE session_nodes SET entry_json = ? WHERE session_key = ?")
        .run(legacy, fixture.sessionKey);
      const migrated = await withDoctorSqliteMaintenanceLock({
        env: state.env,
        operation: "early Doctor cold checkpoint migration",
        run: () =>
          autoMigrateLegacyState({
            cfg: { plugins: { enabled: false } },
            env: state.env,
            homedir: () => state.home,
            doctorOnlyStateMigrations: true,
            invocationPurpose: "doctor",
            legacySessionSurfaces: EMPTY_LEGACY_SESSION_SURFACES,
          }),
      });
      expect(
        migrated.stepReceipts.find((receipt) => receipt.id === "session-entry-state"),
      ).toMatchObject({ outcome: "completed" });
      const after = fixture.read();
      expect(JSON.parse(String(after.raw))).toMatchObject({
        retainedHistoryReferences: { sessionIds: [fixture.sessionId], artifactPaths: [] },
      });
      expect(after.raw).not.toContain("compactionCheckpoints");
      expect(
        JSON.parse(expectDefined(after.event, "restored compaction marker").event_json),
      ).toMatchObject({
        id: "compact",
        summary,
        firstKeptEntryId: "user",
        tokensBefore: 90,
        tokensAfter: 40,
      });
      expect(after.event?.event_json).toContain('"opaqueCount":9007199254740993');
      expect(after.transcriptUpdatedAt).toBe(42);
      const reopened = openOpenClawAgentDatabase({ agentId: "main", env: state.env });
      expect(readSessionColdTranscript(reopened.db, fixture.sessionId)).toBeUndefined();
      const backupName = expectDefined(
        fs
          .readdirSync(path.dirname(fixture.databasePath))
          .find((name) =>
            name.startsWith(`${path.basename(fixture.databasePath)}.pre-startup-migration-`),
          ),
        "cold-history migration backup",
      );
      using backup = new DatabaseSync(path.join(path.dirname(fixture.databasePath), backupName), {
        readOnly: true,
      });
      expect(
        backup
          .prepare("SELECT entry_json FROM session_nodes WHERE session_key = ?")
          .get(fixture.sessionKey)?.entry_json,
      ).toBe(legacy);
      expect(
        backup
          .prepare("SELECT session_id FROM session_transcript_cold_archives WHERE session_id = ?")
          .get(fixture.sessionId)?.session_id,
      ).toBe(fixture.sessionId);
    },
  );
});

it("migrates a shipped server-endpoint checkpoint without fabricating a compaction marker", async () => {
  await withOpenClawTestState(
    { label: "doctor-endpoint-checkpoint", scenario: "minimal" },
    async (state) => {
      const sessionId = "session-endpoint";
      const fixture = seedHistory(
        state,
        "endpoint",
        {
          compactionCheckpoints: [
            {
              ...checkpoint(sessionId),
              checkpointId: "server-checkpoint",
              sessionKey: "agent:main:endpoint",
              createdAt: 42,
              reason: "manual",
              summary: "checkpoint-only endpoint summary",
              postCompaction: { sessionId, leafId: "post-message", entryId: "post-message" },
            },
            {
              ...checkpoint(sessionId),
              checkpointId: "server-checkpoint-without-entry-id",
              sessionKey: "agent:main:endpoint",
              createdAt: 43,
              reason: "manual",
              postCompaction: { sessionId, leafId: "post-message" },
            },
          ],
        },
        "message",
        {
          id: "post-message",
          summary: undefined,
          firstKeptEntryId: undefined,
          tokensBefore: undefined,
          message: { role: "assistant", content: "retained endpoint reply", timestamp: 42 },
        },
      );
      expect(await repair(state)).toMatchObject({ found: 1, repaired: 1 });
      const after = fixture.read();
      expect(after.raw).not.toContain("compactionCheckpoints");
      expect(JSON.parse(String(after.raw))).toMatchObject({
        retainedHistoryReferences: { sessionIds: [sessionId, "previous"], artifactPaths: [] },
      });
      expect(after.event?.event_json).toBe(fixture.eventJson);
      const database = openOpenClawAgentDatabase({ agentId: "main", env: state.env });
      expect(
        database.db
          .prepare(
            "SELECT COUNT(*) AS count FROM transcript_events WHERE session_id = ? AND json_extract(event_json, '$.type') = 'compaction'",
          )
          .get(sessionId)?.count,
      ).toBe(0);
      const backupName = expectDefined(
        fs
          .readdirSync(path.dirname(fixture.databasePath))
          .find((name) =>
            name.startsWith(`${path.basename(fixture.databasePath)}.pre-startup-migration-`),
          ),
        "endpoint checkpoint backup",
      );
      using backup = new DatabaseSync(path.join(path.dirname(fixture.databasePath), backupName), {
        readOnly: true,
      });
      expect(
        backup
          .prepare("SELECT entry_json FROM session_nodes WHERE session_key = ?")
          .get(fixture.sessionKey)?.entry_json,
      ).toBe(fixture.raw);
      expect(
        backup
          .prepare("SELECT event_json FROM transcript_events WHERE session_id = ? AND seq = 3")
          .get(sessionId)?.event_json,
      ).toBe(fixture.eventJson);
    },
  );
});

it.each([
  "duplicate compaction marker",
  "conflicting shared metrics",
  "relative artifact",
  "malformed references",
] as const)(
  "retains original entry and transcript bytes when migration finds %s",
  async (failure) => {
    await withOpenClawTestState(
      { label: "doctor-compaction-refusal", scenario: "minimal" },
      async (state) => {
        const first = seedHistory(state, "a-valid", {
          compactionCheckpoints: [checkpoint("session-a-valid")],
        });
        const invalid = checkpoint(
          failure === "conflicting shared metrics" ? first.sessionId : "session-z-refused",
          failure === "conflicting shared metrics" ? 91 : 90,
        );
        const second = seedHistory(state, "z-refused", {
          pendingFinalDelivery: true,
          compactionCheckpoints:
            failure === "malformed references"
              ? [{ ...invalid, preCompaction: false }]
              : [
                  {
                    ...invalid,
                    preCompaction: {
                      ...invalid.preCompaction,
                      ...(failure === "relative artifact" ? { sessionFile: "relative.jsonl" } : {}),
                    },
                    postCompaction: {
                      ...invalid.postCompaction,
                      entryId: "compact",
                    },
                  },
                ],
        });
        if (failure === "duplicate compaction marker") {
          openOpenClawAgentDatabase({ agentId: "main", env: state.env })
            .db.prepare(
              "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?, 4, ?, 42)",
            )
            .run(second.sessionId, second.eventJson);
        }
        await expect(repair(state)).rejects.toThrow(
          failure === "relative artifact"
            ? /absolute path/
            : failure === "malformed references"
              ? /Invalid legacy compaction history/
              : failure === "conflicting shared metrics"
                ? /Conflicting legacy compaction facts/
                : /Conflicting compaction marker/,
        );
        for (const fixture of [first, second]) {
          const observed = fixture.read();
          expect(observed.raw).toBe(fixture.raw);
          expect(observed.event?.event_json).toBe(fixture.eventJson);
          expect(observed.transcriptUpdatedAt).toBe(42);
        }
      },
    );
  },
);
