package io.disys.jaft.cluster.membership;

import io.disys.jaft.core.NodeId;

/**
 * A single membership change targeting one node.
 *
 * @param id   the node to change
 * @param type the kind of change (add voter, add learner, or remove)
 */
public record MembershipChange(NodeId id, MembershipChangeType type) {
}
