package io.disys.jaft.storage;

import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.core.Snapshot;
import io.disys.jaft.engine.PersistentState;

import java.util.List;

/**
 * Thread-safe wrapper around {@link InMemoryLogStorage}.
 *
 * <p>Every method delegates to the underlying {@link InMemoryLogStorage}
 * under {@code synchronized (this)}, making the instance safe for
 * concurrent access from multiple threads.</p>
 *
 * <p>Intended for tests and environments where a persistent {@link LogStorage}
 * is not needed but concurrent access is required — for example, integration
 * tests that run a live {@code Node} with multiple producer threads.</p>
 */
public final class SynchronizedInMemoryLogStorage implements LogStorage {

    private final InMemoryLogStorage delegate;

    public SynchronizedInMemoryLogStorage() {
        this.delegate = new InMemoryLogStorage();
    }

    public SynchronizedInMemoryLogStorage(Snapshot initialSnapshot, List<Entry> initialEntries) {
        this.delegate = new InMemoryLogStorage(initialSnapshot, initialEntries);
    }

    public SynchronizedInMemoryLogStorage(List<Entry> initialEntries) {
        this.delegate = new InMemoryLogStorage(initialEntries);
    }

    public static SynchronizedInMemoryLogStorage withEntries(List<Entry> entries) {
        return new SynchronizedInMemoryLogStorage(entries);
    }

    public static SynchronizedInMemoryLogStorage withSnapshot(Snapshot snapshot) {
        return new SynchronizedInMemoryLogStorage(
                snapshot,
                List.of(new Entry.Snapshot(snapshot.term(), snapshot.index()))
        );
    }

    // ===================== LogStorage (read interface) ========================

    @Override
    public synchronized InitialState initialState() throws StorageException {
        return delegate.initialState();
    }

    @Override
    public synchronized List<Entry> entries(long low, long high, long maxSize) throws StorageException {
        return delegate.entries(low, high, maxSize);
    }

    @Override
    public synchronized long term(long index) throws StorageException {
        return delegate.term(index);
    }

    @Override
    public synchronized long firstIndex() {
        return delegate.firstIndex();
    }

    @Override
    public synchronized long lastIndex() {
        return delegate.lastIndex();
    }

    @Override
    public synchronized Snapshot snapshot() throws StorageException {
        return delegate.snapshot();
    }

    // ===================== Write operations ===================================

    public synchronized long offset() {
        return delegate.offset();
    }

    public synchronized void setPersistentState(PersistentState persistentState) {
        delegate.setPersistentState(persistentState);
    }

    public synchronized void append(List<Entry> newEntries) throws StorageException {
        delegate.append(newEntries);
    }

    public synchronized void applySnapshot(Snapshot nextSnapshot) throws SnapshotOutOfDateException {
        delegate.applySnapshot(nextSnapshot);
    }

    public synchronized void compact(long compactIndex) throws CompactedException, EntryUnavailableException {
        delegate.compact(compactIndex);
    }

    public synchronized Snapshot createSnapshot(long index, MembershipConfig mc, byte[] data) throws StorageException {
        return delegate.createSnapshot(index, mc, data);
    }
}
