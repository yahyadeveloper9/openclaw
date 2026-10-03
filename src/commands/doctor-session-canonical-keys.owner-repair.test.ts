import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { resolveSessionStorePathCore } from "../config/sessions/paths.js";
import {
  assignSessionOwner,
  loadExactSessionEntryReadOnly,
  loadTranscriptEvents,
  replaceSessionEntrySync,
} from "../config/sessions/session-accessor.js";
import { scanCanonicalSqliteSessionEntries } from "../config/sessions/session-canonical-key.js";
import { resolveSqliteTargetFromSessionStorePath } from "../config/sessions/session-sqlite-target.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { openNodeSqliteDatabase, resolveImmutableSqliteFileUri } from "../infra/node-sqlite.js";
import { FIRST_USE_ADDITIVE_AGENT_COLUMN_DEFINITIONS } from "../state/openclaw-agent-db-additive-columns.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../state/openclaw-agent-db.js";
import { withStateDirEnv } from "../test-helpers/state-dir-env.js";
import { normalizeSessionDeliveryState } from "../utils/delivery-context.shared.js";
import { repairCanonicalSessionKeys } from "./doctor-session-canonical-keys.js";
import { insertLegacySession } from "./doctor-session-canonical-keys.test-support.js";
import { withDoctorSqliteMaintenanceLock } from "./doctor-sqlite-maintenance-lock.js";

afterEach(() => closeOpenClawAgentDatabasesForTest());

function insertEmptyAlias(params: {
  agentId: string;
  env: NodeJS.ProcessEnv;
  sessionId: string;
  sessionKey: string;
  storePath: string;
  updatedAt: number;
}) {
  const database = openOpenClawAgentDatabase({
    agentId: params.agentId,
    env: params.env,
    path: resolveSqliteTargetFromSessionStorePath(params.storePath, {
      agentId: params.agentId,
      env: params.env,
    }).path,
  });
  database.db
    .prepare(
      "INSERT INTO session_nodes (session_key, current_session_id, entry_json, updated_at) VALUES (?, ?, '{}', ?)",
    )
    .run(params.sessionKey, params.sessionId, params.updatedAt);
  return database;
}

describe("doctor transcript owner repair", () => {
  it("canonicalizes retained placeholders without reviving entries or replacing a live owner", async () => {
    await withStateDirEnv("openclaw-doctor-retained-keys-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const storePath = resolveSessionStorePathCore(undefined, { agentId: "main", env });
      const cfg: OpenClawConfig = {
        agents: { ownership: "explicit", entries: { main: {} } },
      };
      const retained = [
        { sourceKey: "retained", canonicalKey: "agent:main:retained", sessionId: "old-retained" },
        { sourceKey: " live ", canonicalKey: "agent:main:live", sessionId: "old-live" },
      ];
      replaceSessionEntrySync(
        { agentId: "main", env, storePath, sessionKey: "agent:main:live" },
        { sessionId: "current-live", updatedAt: 100, label: "Current metadata" },
      );
      replaceSessionEntrySync(
        { agentId: "main", env, storePath, sessionKey: "agent:main:keeper" },
        { sessionId: "keeper", previousSessionId: "older-live", updatedAt: 100 },
      );
      const database = openOpenClawAgentDatabase({
        agentId: "main",
        env,
        path: resolveSqliteTargetFromSessionStorePath(storePath, { agentId: "main", env }).path,
      });
      insertLegacySession({
        agentId: "main",
        env,
        storePath,
        sessionKey: "live",
        entry: { sessionId: "older-live", updatedAt: 10 },
        eventText: "retained by a surviving session",
      });
      database.db
        .prepare("UPDATE session_nodes SET entry_valid = 1 WHERE session_key = 'live'")
        .run();
      for (const { sourceKey, sessionId } of retained) {
        insertLegacySession({
          agentId: "main",
          env,
          storePath,
          sessionKey: sourceKey,
          entry: { sessionId, updatedAt: 20 },
          eventText: "retained history 雪🦞",
        });
        database.db
          .prepare("UPDATE session_nodes SET entry_json = '{}', label = ? WHERE session_key = ?")
          .run(`Retained ${sessionId}`, sourceKey);
        database.db
          .prepare("UPDATE session_nodes SET entry_valid = -1 WHERE session_key = ?")
          .run(sourceKey);
      }
      database.db
        .prepare(
          "INSERT INTO session_entry_snapshots (session_key, field, value_json) VALUES (?, ?, ?)",
        )
        .run("retained", "skillsSnapshot", '{"retained":"opaque original"}');
      database.db
        .prepare("UPDATE session_nodes SET entry_valid = -1 WHERE session_key = 'retained'")
        .run();
      const eventsBefore = database.db
        .prepare("SELECT * FROM transcript_events ORDER BY session_id, seq")
        .all();
      const windowsBefore = database.db
        .prepare("SELECT * FROM session_windows ORDER BY session_id")
        .all();
      const liveBefore = database.db
        .prepare(
          "SELECT entry_json, current_session_id, updated_at, label FROM session_nodes WHERE session_key = ?",
        )
        .get("agent:main:live");
      expect(() => scanCanonicalSqliteSessionEntries(database)).toThrow(
        "run openclaw doctor --fix",
      );
      expect(await repairCanonicalSessionKeys({ apply: false, cfg, env })).toMatchObject({
        foundGroups: 2,
        repairedGroups: 0,
      });
      const repair = () =>
        withDoctorSqliteMaintenanceLock({
          env,
          operation: "test retained transcript repair",
          protectedPaths: [database.path],
          run: (authority) => repairCanonicalSessionKeys({ apply: true, authority, cfg, env }),
        });
      expect(await repair()).toMatchObject({ foundGroups: 2, repairedGroups: 2 });
      expect(
        database.db.prepare("SELECT * FROM transcript_events ORDER BY session_id, seq").all(),
      ).toEqual(eventsBefore);
      expect(
        database.db.prepare("SELECT * FROM session_windows ORDER BY session_id").all(),
      ).toEqual(
        windowsBefore.map((window) => ({
          ...window,
          session_key:
            retained.find((source) => source.sourceKey === window.session_key)?.canonicalKey ??
            (window.session_key === "live" ? "agent:main:live" : window.session_key),
        })),
      );
      expect(
        database.db
          .prepare(
            "SELECT entry_json, current_session_id, updated_at, label FROM session_nodes WHERE session_key = ?",
          )
          .get("agent:main:live"),
      ).toEqual(liveBefore);
      expect(
        database.db
          .prepare(
            "SELECT entry_json, entry_valid, current_session_id, label FROM session_nodes WHERE session_key = ?",
          )
          .get("agent:main:retained"),
      ).toEqual({
        entry_json: "{}",
        entry_valid: -1,
        current_session_id: "old-retained",
        label: "Retained old-retained",
      });
      for (const sourceKey of [...retained.map((source) => source.sourceKey), "live"]) {
        expect(
          database.db
            .prepare("SELECT session_key FROM session_nodes WHERE session_key = ?")
            .get(sourceKey),
        ).toBeUndefined();
      }
      expect(
        database.db
          .prepare("SELECT value_json FROM session_entry_snapshots WHERE session_key = ?")
          .get("agent:main:retained"),
      ).toEqual({ value_json: '{"retained":"opaque original"}' });
      expect(() => scanCanonicalSqliteSessionEntries(database)).not.toThrow();
      expect(await repair()).toMatchObject({ foundGroups: 0, repairedGroups: 0 });
      const backupName = fs
        .readdirSync(path.dirname(database.path))
        .find(
          (name) =>
            name.startsWith(`${path.basename(database.path)}.pre-startup-migration-`) &&
            name.endsWith(".bak"),
        );
      expect(backupName).toBeDefined();
      const backup = openNodeSqliteDatabase(
        resolveImmutableSqliteFileUri(path.join(path.dirname(database.path), backupName!)),
        { readOnly: true },
      );
      try {
        expect(
          backup.prepare("SELECT * FROM transcript_events ORDER BY session_id, seq").all(),
        ).toEqual(eventsBefore);
        expect(backup.prepare("SELECT * FROM session_windows ORDER BY session_id").all()).toEqual(
          windowsBefore,
        );
        expect(
          backup
            .prepare("SELECT entry_json FROM session_nodes WHERE session_key = ?")
            .get(" live "),
        ).toEqual({ entry_json: "{}" });
      } finally {
        backup.close();
      }
    });
  });

  it("moves a qualified retained owner from the wrong store without reviving it", async () => {
    await withStateDirEnv("openclaw-doctor-retained-store-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const sourcePath = resolveSessionStorePathCore(undefined, { agentId: "main", env });
      const destinationPath = resolveSessionStorePathCore(undefined, { agentId: "ops", env });
      const sessionKey = "agent:ops:retained";
      const sessionId = "retained-cross-store";
      const cfg: OpenClawConfig = {
        agents: { ownership: "explicit", entries: { main: {}, ops: {} } },
      };
      insertLegacySession({
        agentId: "main",
        env,
        storePath: sourcePath,
        sessionKey,
        entry: { sessionId, updatedAt: 12 },
        eventText: "preserved cross-store history",
      });
      const source = openOpenClawAgentDatabase({
        agentId: "main",
        env,
        path: resolveSqliteTargetFromSessionStorePath(sourcePath, { agentId: "main", env }).path,
      });
      source.db
        .prepare("UPDATE session_nodes SET entry_json = '{}' WHERE session_key = ?")
        .run(sessionKey);
      source.db
        .prepare("UPDATE session_nodes SET entry_valid = -1 WHERE session_key = ?")
        .run(sessionKey);
      const original = source.db
        .prepare("SELECT * FROM transcript_events WHERE session_id = ? ORDER BY seq")
        .all(sessionId);
      const repair = () =>
        withDoctorSqliteMaintenanceLock({
          env,
          operation: "test retained cross-store repair",
          run: (authority) => repairCanonicalSessionKeys({ apply: true, authority, cfg, env }),
        });
      expect(await repair()).toMatchObject({ foundGroups: 1, repairedGroups: 1 });
      const destination = openOpenClawAgentDatabase({
        agentId: "ops",
        env,
        path: resolveSqliteTargetFromSessionStorePath(destinationPath, { agentId: "ops", env })
          .path,
      });
      expect(
        destination.db
          .prepare("SELECT * FROM transcript_events WHERE session_id = ? ORDER BY seq")
          .all(sessionId),
      ).toEqual(original);
      expect(
        destination.db
          .prepare(
            "SELECT entry_json, entry_valid, current_session_id FROM session_nodes WHERE session_key = ?",
          )
          .get(sessionKey),
      ).toEqual({ entry_json: "{}", entry_valid: -1, current_session_id: sessionId });
      expect(
        destination.db
          .prepare("SELECT session_key FROM session_windows WHERE session_id = ?")
          .get(sessionId),
      ).toEqual({ session_key: sessionKey });
      expect(
        source.db
          .prepare("SELECT session_key FROM session_nodes WHERE session_key = ?")
          .get(sessionKey),
      ).toBeUndefined();
      expect(
        source.db
          .prepare("SELECT session_id FROM session_windows WHERE session_id = ?")
          .get(sessionId),
      ).toBeUndefined();
      expect(() => scanCanonicalSqliteSessionEntries(destination)).not.toThrow();
      expect(await repair()).toMatchObject({ foundGroups: 0, repairedGroups: 0 });
    });
  });

  it("preserves both retained owners when saved snapshots conflict", async () => {
    await withStateDirEnv("openclaw-doctor-retained-key-conflict-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const storePath = resolveSessionStorePathCore(undefined, { agentId: "main", env });
      const cfg: OpenClawConfig = { agents: { ownership: "explicit", entries: { main: {} } } };
      for (const [sessionKey, sessionId] of [
        ["history", "retained"],
        ["agent:main:history", "current"],
      ]) {
        insertLegacySession({
          agentId: "main",
          env,
          storePath,
          sessionKey,
          entry: { sessionId, updatedAt: 1 },
          eventText: sessionId,
        });
      }
      const database = openOpenClawAgentDatabase({
        agentId: "main",
        env,
        path: resolveSqliteTargetFromSessionStorePath(storePath, { agentId: "main", env }).path,
      });
      database.db
        .prepare("UPDATE session_nodes SET entry_json = '{}' WHERE session_key = 'history'")
        .run();
      database.db
        .prepare("UPDATE session_nodes SET entry_valid = -1 WHERE session_key = 'history'")
        .run();
      database.db
        .prepare(
          "INSERT INTO session_entry_snapshots (session_key, field, value_json) VALUES (?, 'skillsSnapshot', ?)",
        )
        .run("history", '{"source":"retained"}');
      database.db
        .prepare(
          "INSERT INTO session_entry_snapshots (session_key, field, value_json) VALUES (?, 'skillsSnapshot', ?)",
        )
        .run("agent:main:history", '{"source":"current"}');
      database.db
        .prepare("UPDATE session_nodes SET entry_valid = -1 WHERE session_key = 'history'")
        .run();
      const nodes = database.db.prepare("SELECT * FROM session_nodes ORDER BY session_key").all();
      const windows = database.db
        .prepare("SELECT * FROM session_windows ORDER BY session_id")
        .all();
      const events = database.db
        .prepare("SELECT * FROM transcript_events ORDER BY session_id, seq")
        .all();
      await expect(repairCanonicalSessionKeys({ apply: true, cfg, env })).rejects.toThrow(
        "conflicts with agent:main:history in skillsSnapshot",
      );
      expect(database.db.prepare("SELECT * FROM session_nodes ORDER BY session_key").all()).toEqual(
        nodes,
      );
      expect(
        database.db.prepare("SELECT * FROM session_windows ORDER BY session_id").all(),
      ).toEqual(windows);
      expect(
        database.db.prepare("SELECT * FROM transcript_events ORDER BY session_id, seq").all(),
      ).toEqual(events);
    });
  });

  it.each([
    { sourceAgentId: "main", requiredAlias: true, requiredCanonical: false },
    { sourceAgentId: "ops", requiredAlias: true, requiredCanonical: false },
    { sourceAgentId: "main", requiredAlias: true, requiredCanonical: true },
    { sourceAgentId: "main", requiredAlias: false, requiredCanonical: false },
  ])("preserves required creation provenance during canonical repair: %o", async (fixture) => {
    await withStateDirEnv("openclaw-doctor-canonical-creation-stamp-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const storeTemplate = path.join(stateDir, "agents", "{agentId}", "sessions.json");
      const destinationStore = resolveSessionStorePathCore(storeTemplate, { agentId: "main", env });
      const sourceStore = resolveSessionStorePathCore(storeTemplate, {
        agentId: fixture.sourceAgentId,
        env,
      });
      const canonicalKey = "agent:main:matrix:channel:!Creation:example.org";
      const cfg: OpenClawConfig = {
        agents: { list: [{ id: "main", default: true }, { id: "ops" }] },
        session: { mainKey: "work", store: storeTemplate },
      };
      const canonicalStamp = {
        createdVia: "operator" as const,
        createdActor: {
          type: "human" as const,
          source: "profile" as const,
          id: "profile-canonical",
        },
        createdAt: 10,
        ...(fixture.requiredCanonical ? { sandbox: "required" as const } : {}),
      };
      const aliasStamp = {
        createdVia: "channel" as const,
        createdActor: { type: "human" as const, source: "channel" as const, id: "profile-alias" },
        createdAt: 20,
        ...(fixture.requiredAlias ? { sandbox: "required" as const } : {}),
      };
      insertLegacySession({
        agentId: "main",
        entry: {
          ...canonicalStamp,
          sessionId: "canonical-session",
          updatedAt: fixture.requiredCanonical ? 10 : 30,
        },
        env,
        sessionKey: canonicalKey,
        storePath: destinationStore,
      });
      insertLegacySession({
        agentId: fixture.sourceAgentId,
        entry: {
          ...aliasStamp,
          delivery: normalizeSessionDeliveryState({
            context: { channel: "matrix", to: "!Creation:example.org" },
          }),
          sessionId: "alias-session",
          updatedAt: 20,
        },
        env,
        sessionKey: canonicalKey.toLowerCase(),
        storePath: sourceStore,
      });

      expect(await repairCanonicalSessionKeys({ apply: true, cfg, env })).toMatchObject({
        foundGroups: 1,
        repairedGroups: 1,
      });
      const repaired = loadExactSessionEntryReadOnly({
        agentId: "main",
        env,
        sessionKey: canonicalKey,
        storePath: destinationStore,
      })?.entry;
      const expectedStamp =
        fixture.requiredAlias && !fixture.requiredCanonical ? aliasStamp : canonicalStamp;
      expect(repaired).toMatchObject({
        ...expectedStamp,
        sessionId: fixture.requiredCanonical ? "alias-session" : "canonical-session",
        updatedAt: fixture.requiredCanonical ? 20 : 30,
      });
      if (!fixture.requiredAlias && !fixture.requiredCanonical) {
        expect(repaired).not.toHaveProperty("sandbox");
      }
    });
  });

  it.each([
    { label: "replaces a stale same-store owner", sourceAgentId: "main", winnerOwned: true },
    { label: "clears a stale same-store owner", sourceAgentId: "main", winnerOwned: false },
    { label: "lazily restores cross-store owner columns", sourceAgentId: "ops", winnerOwned: true },
    {
      label: "preserves an owner while repairing malformed session metadata",
      sourceAgentId: "main",
      winnerOwned: true,
      malformed: true,
    },
  ])("$label from the selected canonical-repair winner", async (fixture) => {
    const { sourceAgentId, winnerOwned } = fixture;
    await withStateDirEnv("openclaw-doctor-assigned-owner-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const storeTemplate = path.join(stateDir, "agents", "{agentId}", "sessions.json");
      const destinationStore = resolveSessionStorePathCore(storeTemplate, { agentId: "main", env });
      const sourceStore = resolveSessionStorePathCore(storeTemplate, {
        agentId: sourceAgentId,
        env,
      });
      const canonicalKey =
        "malformed" in fixture ? "agent:main:main" : "agent:main:matrix:channel:!Owner:example.org";
      const winnerKey = canonicalKey.toLowerCase();
      const cfg = {
        agents: {
          list: [
            { id: "main", default: true },
            ...(sourceAgentId === "ops" ? [{ id: "ops" }] : []),
          ],
        },
        session: { mainKey: "work", store: storeTemplate },
      } as OpenClawConfig;

      if (sourceAgentId === "main" && !("malformed" in fixture)) {
        insertLegacySession({
          agentId: "main",
          entry: { sessionId: "stale-destination", updatedAt: 10 },
          env,
          sessionKey: canonicalKey,
          storePath: destinationStore,
        });
        assignSessionOwner(
          { agentId: "main", env, sessionKey: canonicalKey, storePath: destinationStore },
          {
            owner: { type: "human", id: "profile-stale" },
            assignedBy: { type: "human", id: "profile-stale-assigner" },
            assignedAt: 10,
          },
        );
      }

      insertLegacySession({
        agentId: sourceAgentId,
        entry: { sessionId: "selected-winner", updatedAt: 20 },
        env,
        sessionKey: winnerKey,
        storePath: sourceStore,
      });
      const owner = winnerOwned
        ? assignSessionOwner(
            { agentId: sourceAgentId, env, sessionKey: winnerKey, storePath: sourceStore },
            {
              owner: { type: "human", id: "profile-winner" },
              assignedBy: { type: "agent", id: "research" },
              assignedAt: 1234,
            },
          )
        : undefined;

      openOpenClawAgentDatabase({
        agentId: sourceAgentId,
        env,
        path: resolveSqliteTargetFromSessionStorePath(sourceStore, {
          agentId: sourceAgentId,
          env,
        }).path,
      })
        .db.prepare("UPDATE session_nodes SET entry_json = ? WHERE session_key = ?")
        .run(
          "malformed" in fixture
            ? "{malformed"
            : JSON.stringify({
                sessionId: "selected-winner",
                updatedAt: 20,
                delivery: normalizeSessionDeliveryState({
                  context: { channel: "matrix", to: "!Owner:example.org" },
                }),
              }),
          winnerKey,
        );

      if (sourceAgentId === "ops") {
        const database = openOpenClawAgentDatabase({
          agentId: "main",
          env,
          path: resolveSqliteTargetFromSessionStorePath(destinationStore, {
            agentId: "main",
            env,
          }).path,
        });
        for (const { columnName, tableName } of FIRST_USE_ADDITIVE_AGENT_COLUMN_DEFINITIONS) {
          database.db.exec(`ALTER TABLE ${tableName} DROP COLUMN ${columnName};`);
        }
      }

      expect(await repairCanonicalSessionKeys({ apply: true, cfg, env })).toMatchObject({
        foundGroups: 1,
        repairedGroups: 1,
      });
      expect(
        loadExactSessionEntryReadOnly({
          agentId: "main",
          env,
          sessionKey: canonicalKey,
          storePath: destinationStore,
        })?.entry.owner,
      ).toEqual(owner ?? undefined);
    });
  });

  it("restores a valid node after an empty alias steals its transcript window", async () => {
    await withStateDirEnv("openclaw-doctor-transcript-owner-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const storeTemplate = path.join(stateDir, "agents", "{agentId}", "sessions.json");
      const storePath = resolveSessionStorePathCore(storeTemplate, { agentId: "main", env });
      const cfg = {
        agents: { list: [{ id: "main", default: true }] },
        session: { store: storeTemplate },
      } as OpenClawConfig;
      const canonicalKey = "agent:main:main";
      const staleKey = "agent:main:telegram:default:direct:fixture-peer";
      const sessionId = "stolen-owner-session";
      insertLegacySession({
        agentId: "main",
        entry: { label: "canonical metadata", sessionId, updatedAt: 20 },
        env,
        eventText: "preserved history",
        sessionKey: canonicalKey,
        storePath,
      });
      const database = insertEmptyAlias({
        agentId: "main",
        env,
        sessionId,
        sessionKey: staleKey,
        storePath,
        updatedAt: 30,
      });
      database.db
        .prepare("UPDATE session_nodes SET entry_valid = 1 WHERE session_key = ?")
        .run(canonicalKey);
      database.db
        .prepare("UPDATE session_windows SET session_key = ? WHERE session_id = ?")
        .run(staleKey, sessionId);

      expect(await repairCanonicalSessionKeys({ apply: false, cfg, env })).toMatchObject({
        foundGroups: 1,
        repairedGroups: 0,
      });
      expect(await repairCanonicalSessionKeys({ apply: true, cfg, env })).toMatchObject({
        foundGroups: 1,
        removedRows: 1,
        repairedGroups: 1,
      });
      expect(
        loadExactSessionEntryReadOnly({ agentId: "main", env, sessionKey: staleKey, storePath }),
      ).toBeUndefined();
      expect(
        loadExactSessionEntryReadOnly({ agentId: "main", env, sessionKey: canonicalKey, storePath })
          ?.entry,
      ).toMatchObject({ label: "canonical metadata", sessionId });
      expect(
        database.db
          .prepare("SELECT session_key FROM session_windows WHERE session_id = ?")
          .get(sessionId),
      ).toEqual({ session_key: canonicalKey });
      await expect(
        loadTranscriptEvents({
          agentId: "main",
          env,
          sessionId,
          sessionKey: canonicalKey,
          storePath,
        }),
      ).resolves.toEqual([
        expect.objectContaining({
          message: expect.objectContaining({ content: "preserved history" }),
        }),
      ]);
      expect(await repairCanonicalSessionKeys({ apply: true, cfg, env })).toMatchObject({
        foundGroups: 0,
        repairedGroups: 0,
      });
    });
  });

  it("restores a stolen transcript owner to its literal main key after the main alias changes", async () => {
    await withStateDirEnv("openclaw-doctor-transcript-owner-chain-", async ({ stateDir }) => {
      const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
      const storeTemplate = path.join(stateDir, "agents", "{agentId}", "sessions.json");
      const storePath = resolveSessionStorePathCore(storeTemplate, { agentId: "main", env });
      const cfg = {
        agents: { list: [{ id: "main", default: true }] },
        session: { mainKey: "work", store: storeTemplate },
      } as OpenClawConfig;
      const staleKey = "agent:main:telegram:default:direct:fixture-peer";
      const intermediateKey = "agent:main:main";
      const sessionId = "owner-chain-session";
      insertLegacySession({
        agentId: "main",
        entry: { label: "intermediate metadata", sessionId, updatedAt: 20 },
        env,
        eventText: "chain history",
        sessionKey: intermediateKey,
        storePath,
      });
      insertEmptyAlias({
        agentId: "main",
        env,
        sessionId,
        sessionKey: staleKey,
        storePath,
        updatedAt: 30,
      });

      expect(await repairCanonicalSessionKeys({ apply: true, cfg, env })).toMatchObject({
        foundGroups: 1,
        removedRows: 1,
        repairedGroups: 1,
      });
      expect(
        loadExactSessionEntryReadOnly({ agentId: "main", env, sessionKey: staleKey, storePath }),
      ).toBeUndefined();
      expect(
        loadExactSessionEntryReadOnly({
          agentId: "main",
          env,
          sessionKey: intermediateKey,
          storePath,
        })?.entry,
      ).toMatchObject({ label: "intermediate metadata", sessionId });
      expect(
        loadExactSessionEntryReadOnly({
          agentId: "main",
          env,
          sessionKey: "agent:main:work",
          storePath,
        }),
      ).toBeUndefined();
      await expect(
        loadTranscriptEvents({
          agentId: "main",
          env,
          sessionId,
          sessionKey: intermediateKey,
          storePath,
        }),
      ).resolves.toEqual([
        expect.objectContaining({ message: expect.objectContaining({ content: "chain history" }) }),
      ]);
      expect(await repairCanonicalSessionKeys({ apply: true, cfg, env })).toMatchObject({
        foundGroups: 0,
        repairedGroups: 0,
      });
    });
  });
});
