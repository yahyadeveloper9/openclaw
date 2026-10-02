import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { expectDefined } from "@openclaw/normalization-core";
import { expect, it } from "vitest";
import { loadExactSessionEntry } from "../config/sessions/session-accessor.sqlite-entry.js";
import { loadTranscriptEventsSync } from "../config/sessions/session-accessor.sqlite-read.js";
import { getSessionKysely } from "../config/sessions/session-accessor.sqlite-scope.js";
import { updateSqliteTranscriptEventJsonInTransaction } from "../config/sessions/session-accessor.sqlite-transcript-store.js";
import { runSessionColdStorageMaintenance } from "../config/sessions/session-cold-storage.js";
import { transcriptEventJsonSql } from "../config/sessions/transcript-payload.js";
import { executeSqliteQuerySync } from "../infra/kysely-sync.js";
import { withOpenClawAgentDatabaseReadOnly } from "../state/openclaw-agent-db-readonly.js";
import { runOpenClawAgentWriteTransaction } from "../state/openclaw-agent-db.js";
import { runDoctorSessionSqlite } from "./doctor-session-sqlite.js";
import {
  useDoctorSessionSqliteTestFixture,
  type TestStore,
} from "./doctor-session-sqlite.test-support.js";
import { withDoctorSqliteMaintenanceLock } from "./doctor-sqlite-maintenance-lock.js";

const { createLegacyStore } = useDoctorSessionSqliteTestFixture();

function importCheckpointStore(store: TestStore) {
  return withDoctorSqliteMaintenanceLock({
    env: store.env,
    operation: "checkpoint transcript import",
    run: (authority) =>
      runDoctorSessionSqlite({ env: store.env, mode: "import", store: store.storePath }, authority),
  });
}

it("imports checkpoint metrics with their historical transcript and replays restored sources", async () => {
  const historicalId = "session-history";
  const store = createLegacyStore({
    entryOverrides: {
      compactionCheckpoints: [
        {
          sessionId: "session-1",
          preCompaction: { sessionId: "session-1" },
          postCompaction: { sessionId: historicalId, entryId: "compact" },
          tokensBefore: 90,
          tokensAfter: 40,
          summary: "Obsolete checkpoint summary",
          firstKeptEntryId: "obsolete-entry",
        },
        {
          preCompaction: { sessionId: "session-1" },
          postCompaction: { sessionId: "session-1", entryId: "active-compact" },
          tokensBefore: 100,
          tokensAfter: 50,
        },
      ],
    },
    transcriptLines: [
      '{"type":"session","id":"session-1","version":3}',
      '{"type":"message","id":"active","parentId":null,"message":{"role":"user","content":"Current history"}}',
      '{"type":"compaction","id":"active-compact","parentId":"active","firstKeptEntryId":"active","summary":"Current summary","tokensBefore":600}',
    ],
  });
  const historicalPath = path.join(store.sessionDir, `${historicalId}.jsonl`);
  const unrelatedEvent =
    '{"type":"message","id":"before","parentId":null,"opaque":9007199254740993,"message":{"role":"user","content":"Retained history"}}';
  const header = JSON.stringify({
    type: "session",
    id: historicalId,
    version: 3,
    timestamp: "2026-07-01T00:00:00.000Z",
    cwd: "/fixture",
  });
  const historicalBytes = [
    header,
    unrelatedEvent,
    '{"type":"compaction","id":"compact","parentId":"before","firstKeptEntryId":"before","summary":"Retained summary","tokensBefore":500,"pluginOpaque":{"keep":"exact","opaque":9007199254740993}}',
    "",
  ].join("\n");
  fs.writeFileSync(historicalPath, historicalBytes);
  const originalIndex = fs.readFileSync(store.storePath, "utf8");
  const scope = {
    agentId: "main",
    sessionId: "session-1",
    sessionKey: "agent:main:main",
    storePath: store.storePath,
    env: store.env,
  };

  const imported = await importCheckpointStore(store);
  const readRawHistory = () =>
    withOpenClawAgentDatabaseReadOnly(
      (database) =>
        executeSqliteQuerySync(
          database.db,
          getSessionKysely(database.db)
            .selectFrom("transcript_events")
            .select(transcriptEventJsonSql(database.db).as("event_json"))
            .where("session_id", "=", historicalId)
            .orderBy("seq", "asc"),
        ).rows.map((row) => row.event_json),
      { agentId: "main", env: store.env },
    );

  expect(imported.targets.flatMap((target) => target.issues)).toEqual([]);
  const entry = expectDefined(loadExactSessionEntry(scope)?.entry, "imported session entry");
  expect(entry).not.toHaveProperty("compactionCheckpoints");
  expect(entry?.retainedHistoryReferences).toEqual({
    sessionIds: ["session-1", historicalId],
    artifactPaths: [],
  });
  const events = loadTranscriptEventsSync({ ...scope, sessionId: historicalId });
  expect(events).toHaveLength(3);
  expect(events[2]).toMatchObject({
    type: "compaction",
    id: "compact",
    summary: "Retained summary",
    firstKeptEntryId: "before",
    tokensBefore: 90,
    tokensAfter: 40,
    pluginOpaque: { keep: "exact" },
  });
  const rawHistory = readRawHistory();
  expect(rawHistory).toMatchObject({ found: true });
  if (!rawHistory.found) {
    throw new Error("Expected imported historical transcript");
  }
  expect(rawHistory.value.slice(0, 2)).toEqual([header, unrelatedEvent]);
  expect(rawHistory.value[2]).toContain('"opaque":9007199254740993');
  const indexBackup = expectDefined(
    imported.targets[0]?.archivedLegacyStoreFiles?.[0],
    "legacy index backup",
  );
  expect(fs.readFileSync(indexBackup, "utf8")).toBe(originalIndex);

  const restored = await runDoctorSessionSqlite({
    cfg: {},
    env: store.env,
    mode: "restore",
    store: store.storePath,
  });
  expect(restored.targets.flatMap((target) => target.issues)).toEqual([]);
  expect(fs.readFileSync(store.storePath, "utf8")).toBe(originalIndex);
  expect(fs.readFileSync(historicalPath, "utf8")).toBe(historicalBytes);
  const validated = await runDoctorSessionSqlite({
    env: store.env,
    mode: "validate",
    store: store.storePath,
  });
  expect(validated.targets.flatMap((target) => target.issues)).toEqual([]);
  const sqlitePath = expectDefined(imported.targets[0]?.sqlitePath, "imported database");
  const databaseOptions = { agentId: "main", path: sqlitePath, env: store.env };
  const currentEvents = loadTranscriptEventsSync(scope);
  const hotMarkerJson = expectDefined(rawHistory.value[2], "historical compaction marker");
  runOpenClawAgentWriteTransaction((database) => {
    executeSqliteQuerySync(
      database.db,
      getSessionKysely(database.db)
        .updateTable("session_windows")
        .set({ updated_at: 42, transcript_updated_at: 42 })
        .where("session_id", "=", "session-1"),
    );
    updateSqliteTranscriptEventJsonInTransaction(database, historicalId, [
      {
        seq: 2,
        eventJson: hotMarkerJson.replace('"tokensBefore":90', '"tokensBefore":700'),
      },
    ]);
  }, databaseOptions);
  expect(
    await runSessionColdStorageMaintenance({
      config: {
        agents: { ownership: "explicit", entries: { main: {} } },
        session: {
          store: sqlitePath,
          maintenance: { coldStorage: { enabled: true, afterDays: 30 } },
        },
      },
    }),
  ).toEqual({ archivedTranscripts: 1, externalizedTranscripts: 0 });
  const coldBefore = withOpenClawAgentDatabaseReadOnly(
    (database) =>
      executeSqliteQuerySync(
        database.db,
        getSessionKysely(database.db)
          .selectFrom("session_transcript_cold_archives")
          .selectAll()
          .where("session_id", "=", "session-1"),
      ).rows[0],
    databaseOptions,
  );
  expect(coldBefore).toMatchObject({ found: true, value: { session_id: "session-1" } });
  if (!coldBefore.found || !coldBefore.value) {
    throw new Error("Expected current transcript to be cold before replay");
  }
  expect(coldBefore.value.archive_blob).toBeInstanceOf(Uint8Array);
  const reimported = await importCheckpointStore(store);
  expect(reimported.targets.flatMap((target) => target.issues)).toEqual([]);
  expect(loadExactSessionEntry(scope)?.entry).toEqual(entry);
  expect(loadTranscriptEventsSync({ ...scope, sessionId: historicalId })).toEqual(events);
  expect(readRawHistory()).toEqual(rawHistory);
  expect(loadTranscriptEventsSync(scope)).toEqual(currentEvents);
  const backupName = expectDefined(
    fs
      .readdirSync(path.dirname(sqlitePath))
      .find(
        (name) =>
          name.startsWith(`${path.basename(sqlitePath)}.pre-startup-migration-`) &&
          name.endsWith(".bak"),
      ),
    "destination backup",
  );
  using backup = new DatabaseSync(path.join(path.dirname(sqlitePath), backupName), {
    readOnly: true,
  });
  expect(
    backup
      .prepare("SELECT * FROM session_transcript_cold_archives WHERE session_id = ?")
      .get("session-1"),
  ).toEqual(coldBefore.value);
  expect(
    String(
      backup
        .prepare("SELECT event_json FROM transcript_events WHERE session_id = ? AND seq = 2")
        .get(historicalId)?.event_json,
    ),
  ).toContain('"tokensBefore":700');
});

it("imports retained references without inventing a missing compaction marker", async () => {
  const transcript = [
    { type: "session", id: "session-1", version: 3, timestamp: "", cwd: "/fixture" },
    {
      type: "message",
      id: "current",
      parentId: null,
      message: { role: "user", content: "Current history" },
    },
  ];
  const store = createLegacyStore({
    entryOverrides: {
      compactionCheckpoints: [
        {
          preCompaction: {},
          postCompaction: { sessionId: "session-1", entryId: "missing" },
          tokensBefore: 90,
        },
      ],
    },
    transcriptLines: transcript.map((event) => JSON.stringify(event)),
  });
  const originalIndex = fs.readFileSync(store.storePath, "utf8");
  const scope = {
    agentId: "main",
    sessionId: "session-1",
    sessionKey: "agent:main:main",
    storePath: store.storePath,
    env: store.env,
  };

  const imported = await importCheckpointStore(store);

  expect(imported.targets.flatMap((target) => target.issues)).toEqual([]);
  const entry = loadExactSessionEntry(scope)?.entry;
  expect(entry).not.toHaveProperty("compactionCheckpoints");
  expect(entry?.retainedHistoryReferences).toEqual({
    sessionIds: ["session-1"],
    artifactPaths: [],
  });
  expect(loadTranscriptEventsSync(scope)).toEqual(transcript);
  const indexBackup = expectDefined(
    imported.targets[0]?.archivedLegacyStoreFiles?.[0],
    "legacy index backup",
  );
  expect(fs.readFileSync(indexBackup, "utf8")).toBe(originalIndex);
});
