package io.disys.jaft.storage;

import io.disys.jaft.core.Snapshot;
import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.engine.PersistentState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * In-memory {@link LogStorage} implementation for testing and prototyping.
 *
 * <p>All operations are {@code synchronized} so the instance can be
 * shared across threads, but performance is not a concern - this is
 * not intended for production use.</p>
 *
 * <p>The log is backed by an {@link ArrayList} with an
 * {@link Entry.Snapshot} at position zero marking the snapshot boundary.
 * Compaction discards entries up to a given index and replaces them with
 * a new snapshot sentinel.</p>
 */
public class InMemoryLogStorage implements LogStorage {

    /** The persisted hard state (term and voted-for). */
    private PersistentState persistentState;

    /** The most recent snapshot. */
    private Snapshot snapshot;

    /** The log entries, starting with a placeholder at the snapshot boundary. */
    private List<Entry> entries;

    /**
     * Creates an empty in-memory log with default initial state.
     */
    public InMemoryLogStorage() {
        this(List.of(new Entry.Snapshot(0, 0)));
    }

    /**
     * Creates storage with the given snapshot and log entries.
     *
     * <p>The first entry must be an {@link Entry.Snapshot} whose term and
     * index match the snapshot boundary. Used for restart when loading
     * persisted state.</p>
     *
     * @param initialSnapshot the snapshot (membership and boundary)
     * @param initialEntries  entries starting with a placeholder at the
     *                        snapshot boundary
     * @throws IllegalArgumentException if entries is empty, first entry is
     *         not a Placeholder, or placeholder does not match snapshot boundary
     */
    public InMemoryLogStorage(Snapshot initialSnapshot, List<Entry> initialEntries) {
        if (initialEntries.isEmpty())
            throw new IllegalArgumentException("Entries cannot be empty");

        if (!(initialEntries.getFirst() instanceof Entry.Snapshot(var term, var index)))
            throw new IllegalArgumentException("First entry must be Snapshot");

        if (term != initialSnapshot.term() || index != initialSnapshot.index())
            throw new IllegalArgumentException("Placeholder must match snapshot boundary");

        persistentState = new PersistentState(0, Optional.empty());
        snapshot = initialSnapshot;
        entries = new ArrayList<>(initialEntries);
    }


    /**
     * Creates storage from entries. The first entry must be an
     * {@link Entry.Snapshot} (snapshot boundary sentinel); the snapshot is
     * derived from it.
     *
     * @param initialEntries entries starting with a Placeholder
     * @throws IllegalArgumentException if entries is empty or first entry
     *         is not a Placeholder
     */
    public InMemoryLogStorage(List<Entry> initialEntries) {
        this(snapshotFromEntries(initialEntries), initialEntries);

    }

    /** Derives snapshot from the first (placeholder) entry. */
    private static Snapshot snapshotFromEntries(List<Entry> entries) {
        if (entries.isEmpty())
            throw new IllegalArgumentException("Entries cannot be empty");

        if (entries.getFirst() instanceof Entry.Snapshot(var term, var index))
            return new Snapshot(term, index, MembershipConfig.of(Set.of(), Set.of()), new byte[0]);

        throw new IllegalArgumentException("First entry must be Placeholder");
    }

    /**
     * Creates storage with pre-built entries for testing.
     *
     * <p>Equivalent to {@code new InMemoryLogStorage(entries)}. The first
     * entry must be an {@link Entry.Snapshot}; the snapshot uses empty
     * membership.</p>
     *
     * @param entries entries starting with a Placeholder
     * @return storage with the given entries
     * @throws IllegalArgumentException if entries is empty or first entry
     *         is not a Placeholder
     */
    public static InMemoryLogStorage withEntries(List<Entry> entries) {
        return new InMemoryLogStorage(entries);
    }

    /**
     * Creates storage with only a snapshot (no log entries beyond the boundary).
     *
     * <p>Useful for testing when the log has been compacted to the snapshot
     * or when simulating a freshly loaded snapshot with no subsequent entries.</p>
     *
     * @param snapshot the snapshot (term, index, membership)
     * @return storage with placeholder at the snapshot boundary, no data entries
     */
    public static InMemoryLogStorage withSnapshot(Snapshot snapshot) {
        return new InMemoryLogStorage(snapshot, List.of(new Entry.Snapshot(snapshot.term(), snapshot.index())));
    }

    /** {@inheritDoc} */
    @Override
    public synchronized InitialState initialState() throws StorageException {
        return new InitialState(persistentState, snapshot.membership());
    }

    /**
     * Returns the index of the placeholder entry (snapshot boundary).
     *
     * @return the offset index
     */
    public synchronized long offset() {
        return entries.getFirst().index();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized List<Entry> entries(long low, long high, long maxSize) throws StorageException {
        var offset = offset();
        if (low <= offset)
            throw new CompactedException(low);

        if (high > lastIndex() + 1)
            throw new EntryUnavailableException(high);

        if (entries.size() == 1)
            throw new EntryUnavailableException(low);

        var startIdx = (int) (low - offset);
        var endIdx = (int) (high - offset);

        var result = new ArrayList<Entry>(endIdx - startIdx);
        long totalSize = 0;

        while (startIdx < endIdx) {
            Entry e = entries.get(startIdx);
            long entrySize = e.size();

            if (!result.isEmpty() && totalSize + entrySize > maxSize) {
                break;
            }
            result.add(e);
            totalSize += entrySize;
            startIdx++;
        }


        return result;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized long term(long index) throws StorageException {
        var offset = offset();

        if (index < offset)
            throw new CompactedException(index);

        if (index > lastIndex())
            throw new EntryUnavailableException(index);

        var entryIdx = (int) (index - offset);
        return entries.get(entryIdx).term();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized long firstIndex() {
        return entries.getFirst().index() + 1;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized long lastIndex() {
        return entries.getLast().index();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Snapshot snapshot() throws StorageException {
        return snapshot;
    }

    /**
     * Replaces the persisted hard state.
     *
     * @param persistentState the new hard state
     */
    public synchronized void setPersistentState(PersistentState persistentState) {
        this.persistentState = persistentState;
    }

    /**
     * Appends entries to storage, handling compacted and conflicting entries.
     *
     * <p>Called by the application after receiving
     * {@link io.disys.jaft.engine.RaftOutput#entriesToPersist()}. Handles:</p>
     *
     * <ul>
     *   <li>Entries already compacted - silently skipped</li>
     *   <li>Entries overlapping with existing - existing truncated, new appended</li>
     * </ul>
     *
     * @param newEntries entries to persist
     * @throws StorageException if the append fails
     */
    public synchronized void append(List<Entry> newEntries) throws StorageException {
        if (newEntries.isEmpty())
            return;

        var firstIdx = firstIndex();
        var lastIdx = newEntries.getFirst().index() + newEntries.size() - 1;

        // all entries are already compacted
        if (lastIdx < firstIdx)
            return;

        // truncate entries that are already compacted
        if (firstIdx > newEntries.getFirst().index()) {
            var skip = (int) (firstIdx - newEntries.getFirst().index());
            newEntries = newEntries.subList(skip, newEntries.size());
        }

        var offset = newEntries.getFirst().index() - offset();

        // Truncate conflicting entries - log diverged, newEntries with different term
        while (entries.size() > offset) {
            entries.removeLast();
        }

        entries.addAll(newEntries);
    }


    /**
     * Applies a snapshot, replacing all log entries with the snapshot state.
     *
     * <p>After this call, {@code firstIndex() == nextSnapshot.index() + 1}.</p>
     *
     * @param nextSnapshot the snapshot to apply
     * @throws SnapshotOutOfDateException if the snapshot is older than the current one
     */
    public synchronized void applySnapshot(Snapshot nextSnapshot) throws SnapshotOutOfDateException {
        if (snapshot.index() != 0 && snapshot.index() >= nextSnapshot.index()) {
            throw new SnapshotOutOfDateException(nextSnapshot.index(), snapshot.index());
        }

        snapshot = nextSnapshot;
        entries = new ArrayList<>();
        entries.add(new Entry.Snapshot(snapshot.term(), snapshot.index()));
    }


    /**
     * Discards log entries up to {@code compactIndex} (inclusive).
     *
     * <p>A placeholder is retained at {@code compactIndex} so that
     * {@code term(compactIndex)} remains available for log matching.
     * Only compact up to the applied index - never compact uncommitted
     * entries.</p>
     *
     * @param compactIndex highest index to compact (inclusive)
     * @throws CompactedException        if already compacted past this index
     * @throws EntryUnavailableException if {@code compactIndex > lastIndex()}
     */
    public synchronized void compact(long compactIndex) throws CompactedException, EntryUnavailableException {
        var offset = offset();

        if (compactIndex <= offset)
            throw new CompactedException(compactIndex);

        if (compactIndex > lastIndex())
            throw new EntryUnavailableException(compactIndex);

        var lastCompactEntryIdx = (int) (compactIndex - offset);

        var entriesAfterCompaction = new ArrayList<Entry>(entries.size() - lastCompactEntryIdx + 1);

        entriesAfterCompaction.add(new Entry.Snapshot(entries.get(lastCompactEntryIdx).term(), entries.get(lastCompactEntryIdx).index()));
        entriesAfterCompaction.addAll(entries.subList(lastCompactEntryIdx + 1, entries.size()));

        entries = entriesAfterCompaction;
    }


    /**
     * Creates a snapshot at the given index and replaces the current snapshot.
     *
     * <p>The snapshot uses the term at {@code index} from the log, plus the
     * supplied membership config and data. The index must be strictly after
     * the current snapshot boundary and at or before the last entry.</p>
     *
     * @param index log index at which to create the snapshot
     * @param mc    membership configuration for the snapshot
     * @param data  opaque snapshot payload
     * @return the new snapshot (also stored as the current snapshot)
     * @throws SnapshotOutOfDateException if {@code index} is at or before the
     *         current snapshot boundary
     * @throws EntryUnavailableException if {@code index} is beyond the last entry
     * @throws StorageException          if the read fails
     */
    public synchronized Snapshot createSnapshot(long index, MembershipConfig mc, byte[] data) throws StorageException {
        if (index <= snapshot.index()) {
            throw new SnapshotOutOfDateException(index, snapshot.index());
        }

        if (index > lastIndex()) {
            throw new EntryUnavailableException(index);
        }

        return snapshot = new Snapshot(index, term(index), mc, data);
    }
}
