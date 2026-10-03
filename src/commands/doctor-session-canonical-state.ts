import {
  rehomeSessionDeliveryReferencesForCanonicalRepair,
  type SessionEntryLifecycleRemoval,
} from "../config/sessions/session-accessor.js";
import { rehomeSessionWindows } from "../config/sessions/session-accessor.sqlite-entry-store.js";
import {
  copySessionNodeArtifactsForRepair,
  deleteSessionMembersForRepair,
} from "../config/sessions/session-accessor.sqlite-node-artifacts.js";
import { replaceSessionOwnerInTransaction } from "../config/sessions/session-accessor.sqlite-owner.js";
import type { OpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import type {
  CanonicalSessionCandidate,
  selectCanonicalSessionCandidate,
} from "./doctor-session-canonical-candidates.js";
import { prepareCanonicalRetainedOwner } from "./doctor-session-canonical-retained.js";

export function createCanonicalRepairRemoval(
  candidate: CanonicalSessionCandidate,
  params: {
    archiveRemovedTranscript: boolean;
    deleteOwnedWindows: boolean;
    deliveryCleanupKeys?: readonly string[];
  },
): SessionEntryLifecycleRemoval {
  if (candidate.kind === "retained") {
    return {
      kind: "retained",
      sessionKey: candidate.sessionKey,
      exactStoredKey: true,
      deleteOwnedWindows: params.deleteOwnedWindows,
      ...(params.deliveryCleanupKeys ? { deliveryCleanupKeys: params.deliveryCleanupKeys } : {}),
      expectedSessionId: candidate.sessionId,
      expectedUpdatedAt: candidate.updatedAt,
      expectedRawEntryJson: candidate.rawEntryJson,
      expectedSnapshotRevision: candidate.rawSnapshotRevision,
    };
  }
  const removal = {
    archiveRemovedTranscript: params.archiveRemovedTranscript,
    deleteOwnedWindows: params.deleteOwnedWindows,
    ...(params.deliveryCleanupKeys ? { deliveryCleanupKeys: params.deliveryCleanupKeys } : {}),
    exactStoredKey: true,
    expectedEntry: candidate.expectedEntry,
    sessionKey: candidate.sessionKey,
  } satisfies SessionEntryLifecycleRemoval;
  return candidate.rawEntryJson === undefined
    ? removal
    : Object.assign(removal, {
        expectedRawEntryJson: candidate.rawEntryJson,
        expectedSnapshotRevision: candidate.rawSnapshotRevision,
      });
}

export function createCanonicalDestinationRemovals(
  candidates: readonly CanonicalSessionCandidate[],
  selected: NonNullable<ReturnType<typeof selectCanonicalSessionCandidate>>,
): SessionEntryLifecycleRemoval[] {
  const relatedSessionIds = new Set(
    (selected.kind === "session"
      ? [selected.entry.sessionId, selected.entry.previousSessionId]
      : [selected.winner.sessionId]
    ).filter((value): value is string => typeof value === "string" && value.length > 0),
  );
  return candidates
    .filter(
      (candidate) =>
        candidate.sessionKey !== selected.winner.canonicalKey ||
        (candidate.kind === "session" && candidate.rawEntryJson !== undefined),
    )
    .map((candidate) =>
      createCanonicalRepairRemoval(candidate, {
        archiveRemovedTranscript:
          candidate.kind === "session" && !relatedSessionIds.has(candidate.entry.sessionId),
        deleteOwnedWindows: false,
      }),
    );
}

export function listCanonicalDestinationAliasKeys(
  destinationStore: readonly CanonicalSessionCandidate[],
  winner: CanonicalSessionCandidate,
): string[] {
  return destinationStore
    .map((candidate) => candidate.sessionKey)
    .filter((sessionKey) => sessionKey !== winner.canonicalKey);
}

export function applyCanonicalDestinationArtifacts(params: {
  copyWinnerAlias: boolean;
  database: OpenClawAgentDatabase;
  destinationStore: readonly CanonicalSessionCandidate[];
  rehomeDeliveries: boolean;
  winner: CanonicalSessionCandidate;
}): boolean {
  let created = false;
  const retained = params.destinationStore
    .filter((candidate) => candidate.kind === "retained")
    .toSorted((left, right) => (left === params.winner ? -1 : right === params.winner ? 1 : 0));
  for (const candidate of retained) {
    if (candidate.sessionKey === candidate.canonicalKey) {
      continue;
    }
    created =
      prepareCanonicalRetainedOwner({
        source: params.database,
        destination: params.database,
        candidate,
        canonicalKey: params.winner.canonicalKey,
      }).created || created;
  }
  if (params.winner.kind === "session") {
    replaceSessionOwnerInTransaction(
      params.database,
      params.winner.canonicalKey,
      params.winner.entry.owner,
    );
  }
  const destinationAliasKeys = listCanonicalDestinationAliasKeys(
    params.destinationStore,
    params.winner,
  );
  if (destinationAliasKeys.length > 0) {
    rehomeSessionWindows(params.database, params.winner.canonicalKey, destinationAliasKeys);
    if (params.rehomeDeliveries) {
      rehomeSessionDeliveryReferencesForCanonicalRepair(
        params.database,
        params.winner.canonicalKey,
        destinationAliasKeys,
      );
    }
    copySessionNodeArtifactsForRepair(
      params.database,
      params.database,
      destinationAliasKeys,
      params.winner.canonicalKey,
      { includeMembers: false },
    );
  }
  if (params.copyWinnerAlias && params.winner.sessionKey !== params.winner.canonicalKey) {
    deleteSessionMembersForRepair(params.database, params.winner.canonicalKey);
    copySessionNodeArtifactsForRepair(
      params.database,
      params.database,
      [params.winner.sessionKey],
      params.winner.canonicalKey,
      { includeParticipants: false },
    );
  }
  return created;
}
