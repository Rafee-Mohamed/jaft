package io.disys.jaft.config;

/**
 * Configuration surface required by
 * {@link io.disys.jaft.protocol.role.Candidate} and
 * {@link io.disys.jaft.protocol.role.PreCandidate}.
 */
public interface CandidateConfig {

    /**
     * Base election round timeout in ticks. The actual timeout is
     * randomized in {@code [electionRoundTimeout, 2 * electionRoundTimeout)}
     * to break split-vote livelocks.
     *
     * @return the base election round timeout
     */
    int electionRoundTimeout();
}
