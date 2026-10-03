import {
  applySessionEntryLifecycleMutation,
  copySessionOwnedStateForCanonicalRepair,
  ensureTranscriptGenerationsForCanonicalRepair,
  listSessionGenerationIdsForCanonicalRepair,
  loadTranscriptEvents,
} from "../config/sessions/session-accessor.js";
import { writeTranscriptArchive } from "../config/sessions/session-accessor.sqlite-archive-artifact.js";
import { loadCanonicalRepairEntriesFromDatabase } from "../config/sessions/session-accessor.sqlite-canonical-inventory.js";
import {
  readSqliteSessionGenerationClaim,
  readSqliteSessionGenerationWindows,
  rehomeSqliteSessionGenerationWindow,
} from "../config/sessions/session-accessor.sqlite-generation-copy.js";
import type { SqliteSessionGenerationClaim } from "../config/sessions/session-accessor.sqlite-generation.types.js";
import { collectSessionStateIdsForEntry } from "../config/sessions/session-accessor.sqlite-references.js";
import { resolveSqliteTranscriptArchiveDirectory } from "../config/sessions/session-accessor.sqlite-scope.js";
import { setCanonicalSqliteSessionMainKey } from "../config/sessions/session-canonical-key.js";
import { normalizeStoreSessionKey } from "../config/sessions/store-entry.js";
import { serializeJsonlLines } from "../config/sessions/transcript-jsonl.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { resolveTargetSqliteOptions } from "../infra/session-sqlite-migration-readers.js";
import { runSqliteDeferredTransactionSync } from "../infra/sqlite-transaction.js";
import {
  assertExistingDatabaseIdentity,
  readDatabasePathIdentitySync,
} from "../infra/sqlite-worker-identity.js";
import { withOpenClawAgentDatabaseReadOnly } from "../state/openclaw-agent-db-readonly.js";
import { openOpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import {
  selectCanonicalSessionCandidate,
  type CanonicalSessionCandidate,
} from "./doctor-session-canonical-candidates.js";
import {
  assertRetainedSnapshotsMatchCanonicalEntry,
  prepareCanonicalRetainedOwner,
  finishCanonicalRetainedOwner,
} from "./doctor-session-canonical-retained.js";
import {
  applyCanonicalDestinationArtifacts,
  createCanonicalDestinationRemovals,
  createCanonicalRepairRemoval,
} from "./doctor-session-canonical-state.js";

export async function repairCanonicalSessionGroup(
  candidates: readonly CanonicalSessionCandidate[],
  params: { cfg: OpenClawConfig; env: NodeJS.ProcessEnv; assertCurrent: () => void },
): Promise<string[]> {
  const selected = selectCanonicalSessionCandidate(candidates, params);
  if (!selected) {
    return [];
  }
  await ensureTranscriptGenerationsForCanonicalRepair(
    candidates.filter((candidate) => candidate.kind === "session"),
  );
  const winner = selected.winner;
  const destination = selected.destination;
  const byDatabase = new Map<string, CanonicalSessionCandidate[]>();
  for (const candidate of candidates) {
    const group = byDatabase.get(candidate.sqlitePath) ?? [];
    group.push(candidate);
    byDatabase.set(candidate.sqlitePath, group);
  }

  const destinationStore = byDatabase.get(destination.sqlitePath) ?? [];
  const retainedTransfers = candidates.filter(
    (candidate) => candidate.kind === "retained" && candidate.sqlitePath !== destination.sqlitePath,
  );
  if (retainedTransfers.length > 0) {
    const { restoreSessionColdTranscript } =
      await import("../config/sessions/session-cold-storage.js");
    for (const candidate of retainedTransfers) {
      for (const sessionId of listSessionGenerationIdsForCanonicalRepair({
        agentId: candidate.agentId,
        canonicalKey: candidate.canonicalKey,
        sourceKeys: [candidate.sessionKey],
        storePath: candidate.storePath,
      })) {
        await restoreSessionColdTranscript(
          {
            agentId: candidate.agentId,
            env: params.env,
            storePath: candidate.storePath,
            sessionId,
          },
          params.assertCurrent,
        );
        await restoreSessionColdTranscript(
          {
            agentId: destination.agentId,
            env: params.env,
            storePath: destination.storePath,
            sessionId,
          },
          params.assertCurrent,
        );
      }
    }
  }
  params.assertCurrent();
  const preArchivedDirectories: string[] = [];
  if (winner.kind === "session" && winner.sqlitePath !== destination.sqlitePath) {
    const generationIds = new Set([
      ...listSessionGenerationIdsForCanonicalRepair({
        agentId: winner.agentId,
        canonicalKey: winner.canonicalKey,
        sourceKeys: [winner.sessionKey],
        storePath: winner.storePath,
      }),
      ...collectSessionStateIdsForEntry(winner.entry),
    ]);
    for (const sessionId of generationIds) {
      if (!sessionId) {
        continue;
      }
      const destinationCollision = destinationStore.find(
        (candidate) =>
          (candidate.kind === "session" ? candidate.entry.sessionId : candidate.sessionId) ===
          sessionId,
      );
      const [destinationEvents, sourceEvents] = await Promise.all([
        loadTranscriptEvents({
          agentId: destinationCollision?.agentId ?? destination.agentId,
          sessionId,
          sessionKey: destinationCollision?.sessionKey ?? winner.canonicalKey,
          storePath: destinationCollision?.storePath ?? destination.storePath,
        }),
        loadTranscriptEvents({
          agentId: winner.agentId,
          sessionId,
          sessionKey: winner.sessionKey,
          storePath: winner.storePath,
        }),
      ]);
      params.assertCurrent();
      const destinationContent = serializeJsonlLines(
        destinationEvents.map((event) => JSON.stringify(event)),
      );
      const sourceContent = serializeJsonlLines(sourceEvents.map((event) => JSON.stringify(event)));
      if (!destinationContent || destinationContent === sourceContent) {
        continue;
      }
      const archiveDirectory = resolveSqliteTranscriptArchiveDirectory({
        agentId: destination.agentId,
        env: params.env,
        path: destination.sqlitePath,
      });
      params.assertCurrent();
      writeTranscriptArchive({
        archiveDirectory,
        content: destinationContent,
        reason: "deleted",
        sessionId,
      });
      if (!preArchivedDirectories.includes(archiveDirectory)) {
        preArchivedDirectories.push(archiveDirectory);
      }
    }
  }
  params.assertCurrent();
  setCanonicalSqliteSessionMainKey(
    openOpenClawAgentDatabase(resolveTargetSqliteOptions(destination, params.env)),
    params.cfg.session?.mainKey,
  );
  const destinationIdentity = readDatabasePathIdentitySync(destination.sqlitePath);
  const assertDestinationCurrent = () => {
    params.assertCurrent();
    assertExistingDatabaseIdentity(
      destination.sqlitePath,
      destinationIdentity.key,
      destinationIdentity.birthtime,
    );
  };
  const transferred = new Map<
    CanonicalSessionCandidate,
    { source: SqliteSessionGenerationClaim[]; destination: SqliteSessionGenerationClaim[] }
  >();
  const winnerResult = await applySessionEntryLifecycleMutation({
    agentId: destination.agentId,
    allowCanonicalRepair: true,
    beforeCommitInTransaction: () => {
      assertDestinationCurrent();
      const database = openOpenClawAgentDatabase(
        resolveTargetSqliteOptions(destination, params.env),
      );
      if (destinationStore.length > 0) {
        loadCanonicalRepairEntriesFromDatabase(
          database,
          destinationStore.map((candidate) => candidate.inventoryFact),
        );
      }
      if (selected.kind === "session") {
        for (const candidate of destinationStore) {
          if (candidate.kind === "retained") {
            assertRetainedSnapshotsMatchCanonicalEntry(database, candidate, selected.entry);
          }
        }
      }
    },
    afterUpsertsInTransaction: (destinationDatabase) => {
      assertDestinationCurrent();
      let createdRetainedOwner = false;
      if (winner.kind === "retained" && winner.sqlitePath !== destination.sqlitePath) {
        const source = openOpenClawAgentDatabase(resolveTargetSqliteOptions(winner, params.env));
        runSqliteDeferredTransactionSync(
          source.db,
          () => {
            loadCanonicalRepairEntriesFromDatabase(source, [winner.inventoryFact]);
            createdRetainedOwner = prepareCanonicalRetainedOwner({
              source,
              destination: destinationDatabase,
              candidate: winner,
              canonicalKey: winner.canonicalKey,
            }).created;
          },
          { databaseLabel: source.path, operationLabel: "session canonical retained owner" },
        );
      }
      if (winner.kind === "session" && winner.sqlitePath !== destination.sqlitePath) {
        copySessionOwnedStateForCanonicalRepair({
          canonicalKey: winner.canonicalKey,
          destinationDatabase,
          ...(selected.kind === "session" ? { preferredEntry: selected.entry } : {}),
          preferredSessionKey: winner.sessionKey,
          source: winner,
          sourceEntries: winner.kind === "session" ? [winner.entry] : [],
          sourceKeys: [winner.sessionKey],
        });
      }
      createdRetainedOwner =
        applyCanonicalDestinationArtifacts({
          copyWinnerAlias: winner.sqlitePath === destination.sqlitePath,
          database: destinationDatabase,
          destinationStore,
          rehomeDeliveries: true,
          winner,
        }) || createdRetainedOwner;
      const orderedTransfers = retainedTransfers.toSorted((left, right) =>
        left === winner ? -1 : right === winner ? 1 : 0,
      );
      for (const candidate of orderedTransfers) {
        const source = openOpenClawAgentDatabase(resolveTargetSqliteOptions(candidate, params.env));
        runSqliteDeferredTransactionSync(
          source.db,
          () => {
            loadCanonicalRepairEntriesFromDatabase(source, [candidate.inventoryFact]);
            if (selected.kind === "session") {
              assertRetainedSnapshotsMatchCanonicalEntry(source, candidate, selected.entry);
            }
            const sources = readSqliteSessionGenerationWindows(
              source,
              [candidate.sessionKey],
              [],
            ).map((window) => readSqliteSessionGenerationClaim(source, window));
            const existing = new Map(
              readSqliteSessionGenerationWindows(
                destinationDatabase,
                [],
                sources.map((claim) => claim.window.session_id),
              ).map((window) => [
                window.session_id,
                readSqliteSessionGenerationClaim(destinationDatabase, window),
              ]),
            );
            for (const claim of sources) {
              const current = existing.get(claim.window.session_id);
              const expectedWindow = rehomeSqliteSessionGenerationWindow(
                claim.window,
                winner.canonicalKey,
                new Set([normalizeStoreSessionKey(candidate.sessionKey.trim())]),
              );
              if (
                current &&
                (current.contentFingerprint !== claim.contentFingerprint ||
                  JSON.stringify(current.window) !== JSON.stringify(expectedWindow))
              ) {
                throw new Error(
                  `Retained transcript ${claim.window.session_id} conflicts with its canonical destination; preserve both stores and resolve the history before retrying Doctor.`,
                );
              }
            }
            prepareCanonicalRetainedOwner({
              source,
              destination: destinationDatabase,
              candidate,
              canonicalKey: winner.canonicalKey,
            });
            copySessionOwnedStateForCanonicalRepair({
              canonicalKey: winner.canonicalKey,
              destinationDatabase,
              preserveDestinationMembership: !(createdRetainedOwner && candidate === winner),
              source: candidate,
              sourceEntries: [],
              sourceKeys: [candidate.sessionKey],
            });
            transferred.set(candidate, {
              source: sources,
              destination: readSqliteSessionGenerationWindows(
                destinationDatabase,
                [],
                sources.map((claim) => claim.window.session_id),
              ).map((window) => readSqliteSessionGenerationClaim(destinationDatabase, window)),
            });
          },
          { databaseLabel: source.path, operationLabel: "session canonical retained source" },
        );
      }
      if (candidates.some((candidate) => candidate.kind === "retained")) {
        finishCanonicalRetainedOwner(
          destinationDatabase,
          winner.canonicalKey,
          createdRetainedOwner,
        );
      }
      assertDestinationCurrent();
    },
    removals: createCanonicalDestinationRemovals(destinationStore, selected),
    skipMaintenance: true,
    storePath: destination.storePath,
    upserts:
      selected.kind === "session"
        ? [{ entry: selected.entry, sessionKey: winner.canonicalKey }]
        : [],
  });
  const archivedDirectories = new Set([
    ...preArchivedDirectories,
    ...winnerResult.archivedTranscriptDirectories,
  ]);

  for (const [sqlitePath, storeCandidates] of byDatabase) {
    if (sqlitePath === destination.sqlitePath) {
      continue;
    }
    const storeCandidate = storeCandidates[0]!;
    const result = await applySessionEntryLifecycleMutation({
      agentId: storeCandidate.agentId,
      allowCanonicalRepair: true,
      beforeCommitInTransaction: () => {
        assertDestinationCurrent();
        const database = openOpenClawAgentDatabase(
          resolveTargetSqliteOptions(storeCandidate, params.env),
        );
        loadCanonicalRepairEntriesFromDatabase(
          database,
          storeCandidates.map((candidate) => candidate.inventoryFact),
        );
        for (const candidate of storeCandidates) {
          const copy = transferred.get(candidate);
          if (!copy) {
            continue;
          }
          const current = readSqliteSessionGenerationWindows(
            database,
            [candidate.sessionKey],
            [],
          ).map((window) => readSqliteSessionGenerationClaim(database, window));
          if (
            current.length !== copy.source.length ||
            current.some((claim, index) => claim.fingerprint !== copy.source[index]?.fingerprint)
          ) {
            throw new Error(
              `Retained source history changed for ${candidate.sessionKey}; rerun Doctor before cleanup.`,
            );
          }
          const verified = withOpenClawAgentDatabaseReadOnly(
            (target) => {
              const currentDestination = readSqliteSessionGenerationWindows(
                target,
                [],
                copy.destination.map((claim) => claim.window.session_id),
              ).map((window) => readSqliteSessionGenerationClaim(target, window));
              return (
                currentDestination.length === copy.destination.length &&
                currentDestination.every(
                  (claim, index) => claim.fingerprint === copy.destination[index]?.fingerprint,
                )
              );
            },
            resolveTargetSqliteOptions(destination, params.env),
          );
          if (!verified.found || !verified.value) {
            throw new Error(
              `Retained destination history changed for ${candidate.sessionKey}; preserve the source and rerun Doctor.`,
            );
          }
        }
        assertDestinationCurrent();
      },
      removals: storeCandidates.map((candidate) =>
        createCanonicalRepairRemoval(candidate, {
          archiveRemovedTranscript: true,
          deleteOwnedWindows: true,
          deliveryCleanupKeys: [winner.canonicalKey],
        }),
      ),
      skipMaintenance: true,
      storePath: storeCandidate.storePath,
    });
    // Only the selected winner is copied. Stale loser data survives solely in its
    // verified archive, avoiding an ambiguous cross-store merge contract.
    for (const directory of result.archivedTranscriptDirectories) {
      archivedDirectories.add(directory);
    }
  }
  return [...archivedDirectories];
}
