package io.disys.jaft.storage;

import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.engine.PersistentState;

/**
 * State recovered from {@link LogStorage} during Raft initialization.
 *
 * <p>Combines the persisted hard state (term, vote) with the membership
 * configuration from the most recent snapshot, giving the protocol
 * everything it needs to resume operation.</p>
 *
 * @param persistentState  the persisted hard state (term and voted-for)
 * @param membershipConfig the cluster membership from the latest snapshot
 * @see LogStorage#initialState()
 */
public record InitialState(PersistentState persistentState, MembershipConfig membershipConfig) {
}
