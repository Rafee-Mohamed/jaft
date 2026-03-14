package io.disys.jaft.node.tracker;

import io.disys.jaft.storage.Payload;

/**
 * A {@link Payload} that carries a unique identifier, allowing the
 * {@link DataProposalTracker} to match committed entries back to
 * their originating proposal futures.
 *
 * @param <ID> the identifier type
 */
public interface TrackablePayload<ID> extends Trackable<ID>, Payload {
}
