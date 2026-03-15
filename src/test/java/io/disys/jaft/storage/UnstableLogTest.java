package io.disys.jaft.storage;

import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.core.Snapshot;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class UnstableLogTest {

    /**
     * Builds data entries for test setup. Indices start at {@code index} and increment
     * for each term. Uses {@link InMemoryLogStorageTest.TestPayload} with size 8.
     *
     * @param index starting index for the first entry
     * @param terms term for each entry (one entry per term)
     * @return list of {@link Entry.Data} entries
     */
    static List<Entry> entries(long index, long... terms) {
        var entries = new ArrayList<Entry>();

        for (var term : terms) {
            entries.add(new Entry.Data(term, index++, new InMemoryLogStorageTest.TestPayload(8)));
        }

        return entries;
    }

    /**
     * Builds a snapshot for test setup with empty membership and no data.
     *
     * @param term  term of the snapshot
     * @param index index of the snapshot
     * @return snapshot instance
     */
    static Snapshot snapshot(long term, long index) {
        return new Snapshot(
                term, index, MembershipConfig.of(Set.of(), Set.of()), new byte[0]);
    }

    /* ==================== FIRST INDEX ==================== */

    @Test
    void firstIndexReturnsEmptyWhenNoSnapshot() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 5);
        assertThat(log.firstIndex()).isEmpty();
    }

    @Test
    void firstIndexReturnsSnapshotIndexPlusOneWhenSnapshotPresentWithEntries() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                4,
                snapshot(3, 2),
                false);
        assertThat(log.firstIndex()).hasValue(3);
    }

    @Test
    void firstIndexReturnsSnapshotIndexPlusOneWhenSnapshotPresent() {
        var log = new UnstableLog(snapshot(3, 2));

        assertThat(log.firstIndex()).hasValue(3);
    }

    /* ==================== LAST INDEX ==================== */

    @Test
    void lastIndexReturnsSnapshotIndexWhenNoEntries() {
        var log = new UnstableLog(snapshot(3, 2));
        assertThat(log.lastIndex()).hasValue(2);
    }

    @Test
    void lastIndexReturnsLastEntryIndexWhenEntriesPresent() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                4,
                snapshot(3, 2),
                false);
        assertThat(log.lastIndex()).hasValue(5);
    }

    @Test
    void lastIndexReturnsEmptyWhenNoSnapshotOrEntries() {
        var log = new UnstableLog(0);
        assertThat(log.lastIndex()).isEmpty();
    }

    /* ==================== TERM ==================== */

    @Test
    void termReturnsFromEntries() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                4,
                snapshot(3, 2),
                false);
        assertThat(log.term(4)).hasValue(4);
    }

    @Test
    void termReturnsFromSnapshotWhenNoEntries() {
        var log = new UnstableLog(snapshot(3, 2));
        assertThat(log.term(2)).hasValue(3);
    }

    @Test
    void termReturnsEmptyWhenNoSnapshotOrEntries() {
        var log = new UnstableLog(3);
        assertThat(log.term(4)).isEmpty();
    }

    @Test
    void termReturnsEmptyWhenIndexBeyondLast() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                4,
                snapshot(3, 2),
                false);
        assertThat(log.term(7)).isEmpty();
    }

    @Test
    void termReturnsEmptyWhenIndexBeforeFirst() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                4,
                snapshot(3, 2),
                false);
        assertThat(log.term(1)).isEmpty();
    }

    /* ==================== RESTORE ==================== */

    @Test
    void restoreClearsEntriesAndSetsSnapshot() {
        var snap = snapshot(4, 4);
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.restore(snap);

        assertThat(log.nextSnapshot()).contains(snap);

        assertThat(log.offset()).isEqualTo(5);
        assertThat(log.totalEntriesCount()).isEqualTo(0);
        assertThat(log.entriesToPersistCount()).isEqualTo(0);
    }

    /* ==================== NEXT ENTRIES TO PERSIST ==================== */

    @Test
    void nextEntriesToPersistReturnsAllWhenNoneInProgress() {
        var entries = entries(3, 3, 4, 5);
        var log = new UnstableLog(entries, 3);

        assertThat(log.nextEntriesToPersist())
                .hasSize(3)
                .containsExactlyElementsOf(entries);
    }

    @Test
    void nextEntriesToPersistReturnsSuffixWhenPartiallyInProgress() {
        var entries = entries(3, 3, 4, 5, 6);
        var log = new UnstableLog(entries, 5);

        assertThat(log.nextEntriesToPersist())
                .hasSize(2)
                .containsExactlyElementsOf(entries.subList(2, 4));
    }

    @Test
    void nextEntriesToPersistReturnsEmptyWhenAllInProgress() {
        var entries = entries(3, 3, 4, 5);
        var log = new UnstableLog(entries, 6);

        assertThat(log.nextEntriesToPersist()).isEmpty();
    }

    /* ==================== NEXT SNAPSHOT ==================== */

    @Test
    void nextSnapshotReturnsEmptyWhenNoSnapshot() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 6);

        assertThat(log.nextSnapshot()).isEmpty();
    }

    @Test
    void nextSnapshotReturnsEmptyWhenSnapshotInProgress() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                3,
                snapshot(4, 4),
                true);
        assertThat(log.nextSnapshot()).isEmpty();
    }

    @Test
    void nextSnapshotReturnsSnapshotWhenPresentAndNotInProgress() {
        var snapshot = snapshot(4, 4);
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                3,
                snapshot,
                false);

        assertThat(log.nextSnapshot()).contains(snapshot);
    }

    /* ==================== ACCEPT IN PROGRESS ==================== */

    @Test
    void acceptInProgressMarksEntriesAndSnapshot() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                3,
                snapshot(4, 4),
                false);
        log.acceptInProgress();

        assertThat(log.nextEntriesToPersist()).isEmpty();
        assertThat(log.nextSnapshot()).isEmpty();
        assertThat(log.snapshotInProgress()).isTrue();
    }

    /* ==================== STABLE TO ==================== */

    @Test
    void stableToRemovesEntriesAndAdvancesOffset() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.stableTo(4, 4);

        assertThat(log.offset()).isEqualTo(5);
        assertThat(log.totalEntriesCount()).isEqualTo(1);
    }

    @Test
    void stableToIgnoresWhenNoSnapshotOrEntries() {
        var log = new UnstableLog(3);
        log.stableTo(3, 4);

        assertThat(log.offset()).isEqualTo(4);
        assertThat(log.totalEntriesCount()).isZero();
    }

    @Test
    void stableToRemovesOnlyEntryAndAdvancesOffset() {
        var log = new UnstableLog(entries(3, 3), 3);
        log.stableTo(3, 3);

        assertThat(log.offset()).isEqualTo(4);
        assertThat(log.totalEntriesCount()).isZero();
    }

    @Test
    void stableToPreservesPersistingUpToWhenAheadOfOffset() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 6);
        log.stableTo(4, 4);

        assertThat(log.offset()).isEqualTo(5);
        assertThat(log.nextEntriesToPersist()).isEmpty();

    }

    @Test
    void stableToIgnoresTermMismatch() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.stableTo(4, 5);

        assertThat(log.offset()).isEqualTo(3);
        assertThat(log.nextEntriesToPersist()).hasSize(2);
    }

    @Test
    void stableToIgnoresIndexBeforeOffset() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.stableTo(3, 2);

        assertThat(log.offset()).isEqualTo(3);
    }

    /* ==================== STABLE TO WITH SNAPSHOT ==================== */

    @Test
    void stableToIgnoresWhenIndexMatchesSnapshot() {
        var log = new UnstableLog(snapshot(3, 4));
        log.stableTo(3, 4);

        assertThat(log.offset()).isEqualTo(5);
    }

    /* ==================== STABLE SNAPSHOT TO ==================== */

    @Test
    void stableSnapshotToClearsSnapshotWhenIndexMatches() {
        var log = new UnstableLog(
                entries(3, 3, 4, 5),
                3,
                snapshot(4, 4),
                true);

        log.stableSnapshotTo(4);
        assertThat(log.nextSnapshot()).isEmpty();
        assertThat(log.snapshotInProgress()).isFalse();
    }

    /* ==================== APPEND ==================== */

    @Test
    void appendAddsToTailWhenNewEntriesFollowLastIndex() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.append(entries(6, 6, 7));

        assertThat(log.offset()).isEqualTo(3);
        assertThat(log.totalEntriesCount()).isEqualTo(5);
    }

    @Test
    void appendReplacesAllWhenNewEntriesStartBeforeOffset() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.append(entries(2, 4, 5));

        assertThat(log.offset()).isEqualTo(2);
        assertThat(log.totalEntriesCount()).isEqualTo(2);
    }

    @Test
    void appendTruncatesConflictingSuffixAndAppendsWhenOverlap() {
        var log = new UnstableLog(entries(3, 3, 4, 5), 4);
        log.append(entries(4, 5));

        assertThat(log.offset()).isEqualTo(3);
        assertThat(log.totalEntriesCount()).isEqualTo(2);
    }

}
