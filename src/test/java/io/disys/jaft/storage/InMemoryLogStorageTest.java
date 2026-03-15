package io.disys.jaft.storage;

import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.core.Snapshot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

public class InMemoryLogStorageTest {

    /** Test payload with configurable size for log entries. */
    record TestPayload(long bytes) implements Payload {}

    /**
     * Builds entries for test setup: a placeholder at the snapshot boundary followed
     * by data entries. Indices start at {@code placeholderIndex + 1} and increment.
     *
     * @param placeholderIndex index of the placeholder (snapshot boundary)
     * @param placeholderTerm  term of the placeholder
     * @param terms            terms for each subsequent data entry
     * @return list with Placeholder first, then Data entries (payload size 8)
     */
    static List<Entry> entries(long placeholderIndex, long placeholderTerm, long... terms) {
        var entries = new ArrayList<Entry>();
        entries.add(new Entry.Placeholder(placeholderTerm, placeholderIndex));

        var idx = placeholderIndex + 1;
        for (var term: terms) {
            entries.add(new Entry.Data(term, idx++, new TestPayload(8)));
        }

        return entries;
    }

    InMemoryLogStorage storage;

    @BeforeEach
    void setUp() {
        // [(term=3, idx=3), (term=4, idx=4), (term=5, idx=5), (term=6, idx=6)]
        storage = InMemoryLogStorage.withEntries(entries(3, 3, 4, 5, 6));
    }


    /* ==================== TERM ==================== */

    @Test
    void termReturnsCorrectValueForValidIndices() throws StorageException {
        assertThat(storage.term(3)).isEqualTo(3);
        assertThat(storage.term(4)).isEqualTo(4);
        assertThat(storage.term(5)).isEqualTo(5);
    }

    @Test
    void termThrowsCompactedForIndexBeforeFirst() {
        assertThatThrownBy(() -> storage.term(2)).isInstanceOf(CompactedException.class);
    }

    @Test
    void termThrowsEntryUnavailableForIndexBeyondLast() {
        assertThatThrownBy(() -> storage.term(7)).isInstanceOf(EntryUnavailableException.class);
    }

    /* ==================== FIRST INDEX / LAST INDEX ==================== */

    @Test
    void firstIndexReturnsPlaceholderIndexPlusOne() {
        assertThat(storage.firstIndex()).isEqualTo(4);
    }

    @Test
    void lastIndexReturnsLastEntryIndex() {
        assertThat(storage.lastIndex()).isEqualTo(6);
    }


    @Test
    void lastIndexUpdatesAfterAppend() throws StorageException {
        assertThat(storage.lastIndex()).isEqualTo(6);

        storage.append(List.of(new Entry.Data(6, 7, new TestPayload(8))));
        assertThat(storage.lastIndex()).isEqualTo(7);
    }


    @Test
    void firstIndexUpdatesAfterCompact() throws StorageException {
        assertThat(storage.firstIndex()).isEqualTo(4);

        storage.compact(4);
        assertThat(storage.firstIndex()).isEqualTo(5);
    }

    /* ==================== COMPACT ==================== */

    @Test
    void compactThrowsCompactedForIndexAtOrBeforeOffset() {
        assertThatThrownBy(() -> storage.compact(2))
                .isInstanceOf(CompactedException.class);

        assertThatThrownBy(() -> storage.compact(3))
                .isInstanceOf(CompactedException.class);
    }

    @Test
    void compactRetainsPlaceholderAndRemainingEntries() throws StorageException {
        storage.compact(4);

        assertThat(storage.offset()).isEqualTo(4);
        assertThat(storage.firstIndex()).isEqualTo(5);
        assertThat(storage.lastIndex()).isEqualTo(6);
        assertThat(storage.term(4)).isEqualTo(4);
    }

    @Test
    void compactThrowsEntryUnavailableForIndexBeyondLast() {
        assertThatThrownBy(() -> storage.compact(7))
                .isInstanceOf(EntryUnavailableException.class);
    }

    @Test
    void compactToLastIndexLeavesOnlyPlaceholder() throws StorageException {
        storage.compact(6);

        assertThat(storage.firstIndex()).isEqualTo(7);
        assertThat(storage.lastIndex()).isEqualTo(6);
        assertThat(storage.term(6)).isEqualTo(6);
    }

    /* ==================== ENTRIES ==================== */

    @Test
    void entriesReturnsRangeWithMaxSizeLimit() throws StorageException {
        var payload = new TestPayload(8);

        assertThat(storage.entries(4, 5, Long.MAX_VALUE))
                .hasSize(1)
                .containsExactly(new Entry.Data(4, 4, payload));

        assertThat(storage.entries(4, 7, Long.MAX_VALUE))
                .hasSize(3)
                .containsExactly(
                        new Entry.Data(4, 4, payload),
                        new Entry.Data(5, 5, payload),
                        new Entry.Data(6, 6, payload));
    }

    @Test
    void entriesThrowsCompactedForLowAtOrBeforeOffset() {
        assertThatThrownBy(() -> storage.entries(3, 6, Long.MAX_VALUE))
                .isInstanceOf(CompactedException.class);

        assertThatThrownBy(() -> storage.entries(2, 6, Long.MAX_VALUE))
                .isInstanceOf(CompactedException.class);
    }

    @Test
    void entriesThrowsEntryUnavailableForHighBeyondLast() {
        assertThatThrownBy(() -> storage.entries(4, 8, Long.MAX_VALUE))
                .isInstanceOf(EntryUnavailableException.class);
    }

    @Test
    void entriesReturnsFirstEntryWhenMaxSizeZero() throws StorageException {
        assertThat(storage.entries(4, 7, 0))
                .hasSize(1)
                .containsExactly(new Entry.Data(4, 4, new TestPayload(8)));
    }

    @Test
    void entriesRespectMaxSizeLimit() throws StorageException {
        assertThat(storage.entries(4, 7, 8)).hasSize(1);
        assertThat(storage.entries(4, 7, 16)).hasSize(2);
        assertThat(storage.entries(4, 7, 20)).hasSize(2);
    }

    @Test
    void entriesThrowsEntryUnavailableWhenOnlyPlaceholder() {
        var snapshot = new Snapshot(
                3,
                3,
                MembershipConfig.of(Set.of(), Set.of()),
                new byte[0]
        );
        var storage = InMemoryLogStorage.withSnapshot(snapshot);

        assertThatThrownBy(() -> storage.entries(4, 5, Long.MAX_VALUE))
                .isInstanceOf(EntryUnavailableException.class);
    }

    /* ==================== APPEND ==================== */

    @Test
    void appendDirectAppendsToTailWhenNoOverlap() throws StorageException {
        storage.append(List.of(new Entry.Data(6, 7, new TestPayload(8))));

        assertThat(storage.entries(4, 8, Long.MAX_VALUE)).hasSize(4);
        assertThat(storage.lastIndex()).isEqualTo(7);
        assertThat(storage.term(7)).isEqualTo(6);
    }

    @Test
    void appendTruncatesAndOverwritesWhenTermDiverges() throws StorageException {
        // truncate from index 4, removes from term(4), index(4) and appends these
        storage.append(List.of(
                new Entry.Data(6, 4, new TestPayload(8)),
                new Entry.Data(6, 5, new TestPayload(8)),
                new Entry.Data(6, 6, new TestPayload(8))));

        assertThat(storage.term(4)).isEqualTo(6);
        assertThat(storage.term(5)).isEqualTo(6);
        assertThat(storage.term(6)).isEqualTo(6);
    }

    @Test
    void appendWithSameEntriesLeavesStorageUnchanged() throws StorageException {
        var first = new Entry.Data(4, 4, new TestPayload(8));
        var second = new Entry.Data(5, 5, new TestPayload(8));
        var third = new Entry.Data(6, 6, new TestPayload(8));

        storage.append(List.of(first, second, third));

        assertThat(storage.entries(4, 7, Long.MAX_VALUE))
                .hasSize(3)
                .containsExactly(first, second, third);
    }

    @Test
    void appendSkipsWhenAllEntriesAlreadyCompacted() throws StorageException {
        storage.append(List.of(
                new Entry.Data(1, 1, new TestPayload(8)),
                new Entry.Data(2, 2, new TestPayload(8))
        ));

        assertThat(storage.firstIndex()).isEqualTo(4);
        assertThat(storage.lastIndex()).isEqualTo(6);
    }

    /* ==================== CREATE SNAPSHOT / APPLY SNAPSHOT ==================== */

    @Test
    void createSnapshotThrowsSnapshotOutOfDateForIndexAtOrBeforeFirst() {
        var mc =  MembershipConfig.of(Set.of(), Set.of());
        var data = new byte[0];

        assertThatThrownBy(() -> storage.createSnapshot(3, mc, data))
                .isInstanceOf(SnapshotOutOfDateException.class);

        assertThatThrownBy(() -> storage.createSnapshot(2, mc, data))
                .isInstanceOf(SnapshotOutOfDateException.class);
    }

    @Test
    void createSnapshotThrowsEntryUnavailableForIndexBeyondLast() {
        var mc =  MembershipConfig.of(Set.of(), Set.of());
        var data = new byte[0];

        assertThatThrownBy(() -> storage.createSnapshot(7, mc, data))
                .isInstanceOf(EntryUnavailableException.class);
    }

    @Test
    void createSnapshotReturnsSnapshotWithCorrectTermAndIndex() throws StorageException {
        var mc =  MembershipConfig.of(Set.of(), Set.of());
        var data = new byte[0];

        var snapshot = storage.createSnapshot(5, mc, data);

        assertThat(snapshot.index()).isEqualTo(5);
        assertThat(snapshot.term()).isEqualTo(5);
    }

    @Test
    void applySnapshotReplacesLog() throws StorageException {
        var snapshot = new Snapshot(
                6,
                6,
                MembershipConfig.of(Set.of(), Set.of()),
                new byte[0]
        );

        storage.applySnapshot(snapshot);

        assertThat(storage.firstIndex()).isEqualTo(7);
        assertThat(storage.lastIndex()).isEqualTo(6);
    }

    @Test
    void applySnapshotThrowsWhenOlderThanCurrent() {
        var snapshot = new Snapshot(
                3,
                3,
                MembershipConfig.of(Set.of(), Set.of()),
                new byte[0]
        );
        var storage = InMemoryLogStorage.withSnapshot(snapshot);

        assertThatThrownBy(() -> storage.applySnapshot(
                new Snapshot(
                        2,
                        2,
                        MembershipConfig.of(Set.of(), Set.of()),
                        new byte[0]
                )
        )).isInstanceOf(SnapshotOutOfDateException.class);
    }

}
