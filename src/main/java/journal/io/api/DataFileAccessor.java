/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package journal.io.api;

import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import journal.io.api.Journal.WriteCommand;
import journal.io.util.IOHelper;
import static journal.io.util.LogHelper.*;

/**
 * File reader/updater to randomly access data files, supporting concurrent
 * thread-isolated reads and writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAccessor {

    private final ConcurrentMap<Thread, ConcurrentMap<Integer, RandomAccessFile>> perThreadDataFileRafs = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, RandomAccessFile>>();
    private final ConcurrentMap<Thread, ConcurrentMap<Integer, Lock>> perThreadDataFileLocks = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, Lock>>();
    private final ReadWriteLock accessorLock = new ReentrantReadWriteLock();
    private final Lock shared = accessorLock.readLock();
    private final Lock exclusive = accessorLock.writeLock();
    private volatile boolean opened;
    //
    private final Journal journal;
    //    
    private volatile ScheduledExecutorService disposer;

    public DataFileAccessor(Journal journal) {
        this.journal = journal;
    }

    void updateLocation(Location location, byte type, boolean sync) throws ClosedJournalException, CompactedDataFileException, IOException {
        Lock threadLock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
        shared.lock();
        threadLock.lock();
        try {
            if (opened) {
                if (journal.getInflightWrites().containsKey(location)) {
                    journal.sync();
                }
                //
                RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
                if (seekToLocation(raf, location, false, false)) {
                    int pointer = raf.readInt();
                    int size = raf.readInt();
                    location.setType(type);
                    raf.write(type);
                    if (sync) {
                        IOHelper.sync(raf.getFD());
                    }
                    IOHelper.skipBytes(raf, size - Journal.RECORD_HEADER_SIZE);
                } else {
                    throw new IOException("Cannot find location: " + location);
                }
            } else {
                throw new ClosedJournalException("The journal is closed!");
            }
        } finally {
            threadLock.unlock();
            shared.unlock();
        }
    }

    void deleteFromLocation(Location source) throws ClosedJournalException, CompactedDataFileException, IOException {
        Lock threadLock = getOrCreateLock(Thread.currentThread(), source.getDataFileId());
        shared.lock();
        threadLock.lock();
        try {
            if (opened) {
                if (journal.getInflightWrites().containsKey(source)) {
                    journal.sync();
                }
                //
                RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), source.getDataFileId());
                long position = source.getThisFilePosition();
                if (position != Location.NOT_SET) {
                    DataFile dataFile = journal.getDataFile(source.getDataFileId());
                    raf.setLength(position);
                    IOHelper.sync(raf.getFD());
                    dataFile.setLength(position);
                } else {
                    throw new IOException("Cannot find location: " + source);
                }
            } else {
                throw new ClosedJournalException("The journal is closed!");
            }
        } finally {
            threadLock.unlock();
            shared.unlock();
        }
    }

    byte[] readLocation(Location location, boolean sync) throws IOException {
        if (location.getData() != null && !sync) {
            return location.getData();
        } else {
            Location read = readLocationDetails(location.getDataFileId(), location.getPointer());
            if (read != null && !read.isDeletedRecord()) {
                return read.getData();
            } else {
                throw new IOException("Invalid location: " + location + ", found: " + read);
            }
        }
    }

    Location readLocationDetails(int file, int pointer) throws ClosedJournalException, IOException {
        WriteCommand asyncWrite = journal.getInflightWrites().get(new Location(file, pointer));
        if (asyncWrite != null) {
            Location location = new Location(file, pointer);
            location.setPointer(asyncWrite.getLocation().getPointer());
            location.setSize(asyncWrite.getLocation().getSize());
            location.setType(asyncWrite.getLocation().getType());
            location.setData(asyncWrite.getData());
            return location;
        } else {
            Location location = new Location(file, pointer);
            Lock threadLock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
            shared.lock();
            threadLock.lock();
            try {
                if (opened) {
                    RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
                    if (seekToLocation(raf, location, true, true)) {
                        if (location.getSize() > 0) {
                            location.setDataFileGeneration(journal.getDataFile(file).getDataFileGeneration());
                            location.setNextFilePosition(raf.getFilePointer());
                            return location;
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                } else {
                    throw new ClosedJournalException("The journal is closed!");
                }
            } catch (CompactedDataFileException ex) {
                warn(ex.getMessage());
                return null;
            } finally {
                threadLock.unlock();
                shared.unlock();
            }
        }
    }

    Location readNextLocationDetails(Location start, final int type) throws ClosedJournalException, IOException {
        // Try with the most immediate subsequent location among inflight writes:
        Location asyncLocation = new Location(start.getDataFileId(), start.getPointer() + 1);
        WriteCommand asyncWrite = journal.getInflightWrites().get(asyncLocation);
        if (asyncWrite != null && asyncWrite.getLocation().isBatchControlRecord() && type != Location.BATCH_CONTROL_RECORD_TYPE) {
            asyncLocation = new Location(start.getDataFileId(), start.getPointer() + 2);
            asyncWrite = journal.getInflightWrites().get(asyncLocation);
        }
        if (asyncWrite != null) {
            asyncLocation.setPointer(asyncWrite.getLocation().getPointer());
            asyncLocation.setSize(asyncWrite.getLocation().getSize());
            asyncLocation.setType(asyncWrite.getLocation().getType());
            asyncLocation.setData(asyncWrite.getData());
            return asyncLocation;
        } else if (!journal.getInflightWrites().containsKey(start)) {
            // Else read from file:
            Lock threadLock = getOrCreateLock(Thread.currentThread(), start.getDataFileId());
            shared.lock();
            threadLock.lock();
            try {
                if (opened) {
                    RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), start.getDataFileId());
                    if (isIntoNextLocation(raf, start) || seekToLocation(raf, start, true, false)) {
                        if (hasRecordHeader(raf, raf.getFilePointer())) {
                            Location next = new Location(start.getDataFileId());
                            do {
                                next.setThisFilePosition(raf.getFilePointer());
                                next.setPointer(raf.readInt());
                                next.setSize(raf.readInt());
                                next.setType(raf.readByte());
                                if (type != Location.ANY_RECORD_TYPE && next.getType() != type) {
                                    boolean skipped = skipLocationData(raf, next);
                                    assert skipped;
                                } else {
                                    break;
                                }
                            } while (hasRecordHeader(raf, raf.getFilePointer()));
                            if (type == Location.ANY_RECORD_TYPE || next.getType() == type) {
                                next.setData(readLocationData(next, raf));
                                next.setDataFileGeneration(journal.getDataFile(start.getDataFileId()).getDataFileGeneration());
                                next.setNextFilePosition(raf.getFilePointer());
                                return next;
                            } else {
                                raf.seek(0);
                                return null;
                            }
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                } else {
                    throw new ClosedJournalException("The journal is closed!");
                }
            } catch (CompactedDataFileException ex) {
                warn(ex.getMessage());
                return null;
            } finally {
                threadLock.unlock();
                shared.unlock();
            }
        } else {
            return null;
        }
    }

    void dispose(DataFile dataFile) {
        Integer dataFileId = dataFile.getDataFileId();
        for (Entry<Thread, ConcurrentMap<Integer, RandomAccessFile>> threadRafs : perThreadDataFileRafs.entrySet()) {
            for (Entry<Integer, RandomAccessFile> raf : threadRafs.getValue().entrySet()) {
                if (raf.getKey().equals(dataFileId)) {
                    disposeByThread(threadRafs.getKey(), dataFileId);
                    break;
                }
            }
        }
    }

    void open() {
        disposer = journal.getDisposer();
        disposer.scheduleAtFixedRate(new ResourceDisposer(), journal.getDisposeInterval(), journal.getDisposeInterval(), TimeUnit.MILLISECONDS);
        opened = true;
    }

    void close() {
        exclusive.lock();
        try {
            opened = false;
            for (Entry<Thread, ConcurrentMap<Integer, RandomAccessFile>> threadRafs : perThreadDataFileRafs.entrySet()) {
                for (Entry<Integer, RandomAccessFile> raf : threadRafs.getValue().entrySet()) {
                    disposeByThread(threadRafs.getKey(), raf.getKey());
                }
            }
        } finally {
            exclusive.unlock();
        }
    }

    void pause() throws ClosedJournalException {
        if (opened) {
            exclusive.lock();
        } else {
            throw new ClosedJournalException("The journal is closed!");
        }
    }

    void resume() throws ClosedJournalException {
        if (opened) {
            exclusive.unlock();
        } else {
            throw new ClosedJournalException("The journal is closed!");
        }
    }

    private void disposeByThread(Thread t, Integer dataFileId) {
        Lock lock = getOrCreateLock(t, dataFileId);
        lock.lock();
        try {
            removeRaf(t, dataFileId);
        } catch (IOException ex) {
            warn(ex, ex.getMessage());
        } finally {
            lock.unlock();
        }
    }

    private boolean isIntoNextLocation(RandomAccessFile raf, Location source) throws CompactedDataFileException, IOException {
        int generation = journal.getDataFile(source.getDataFileId()).getDataFileGeneration();
        long position = raf.getFilePointer();
        return source.getDataFileGeneration() == generation
                && source.getNextFilePosition() == position;
    }

    private boolean seekToLocation(RandomAccessFile raf, Location destination, boolean fillLocation, boolean fillData) throws IOException {
        // First try the next file position:
        long position = raf.getFilePointer();
        int pointer = -1;
        int size = -1;
        byte type = -1;
        if (hasRecordHeader(raf, position)) {
            pointer = raf.readInt();
            size = raf.readInt();
            type = raf.readByte();
        }
        // If pointer is wrong, seek by trying the saved file position:
        if (pointer != destination.getPointer()) {
            position = destination.getThisFilePosition();
            if (position != Location.NOT_SET && hasRecordHeader(raf, position)) {
                raf.seek(position);
                pointer = raf.readInt();
                size = raf.readInt();
                type = raf.readByte();
            }
            // Else seek by using hints:
            if (pointer != destination.getPointer()) {
                Entry<Location, Long> hint = journal.getHints().lowerEntry(destination);
                if (hint != null && hint.getKey().getDataFileId() == destination.getDataFileId()) {
                    position = hint.getValue();
                } else {
                    position = Journal.FILE_HEADER_SIZE;
                }
                raf.seek(position);
                if (hasRecordHeader(raf, position)) {
                    pointer = raf.readInt();
                    size = raf.readInt();
                    type = raf.readByte();
                    while (pointer != destination.getPointer()) {
                        IOHelper.skipBytes(raf, size - Journal.RECORD_HEADER_SIZE);
                        position = raf.getFilePointer();
                        if (hasRecordHeader(raf, position)) {
                            pointer = raf.readInt();
                            size = raf.readInt();
                            type = raf.readByte();
                        } else {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
        }
        // Filling data implies filling location metadata too:
        if (fillLocation || fillData) {
            // Fill location metadata:
            destination.setThisFilePosition(position);
            destination.setSize(size);
            destination.setType(type);
            // Maybe fill data too:
            if (fillData) {
                destination.setData(readLocationData(destination, raf));
                // Do not seek back to this location position so next read
                // can be sequential.
            } else {
                // Otherwise skip location data, so again next read can be
                // sequential
                boolean skipped = skipLocationData(raf, destination);
                assert skipped;
            }
        } else {
            // Otherwise get back to the header position, as the caller may be interested
            // in reading:
            raf.seek(position);
        }
        return true;
    }

    private boolean skipLocationData(RandomAccessFile raf, Location source) throws IOException {
        int toSkip = source.getSize() - Journal.RECORD_HEADER_SIZE;
        if (raf.length() - raf.getFilePointer() >= toSkip) {
            IOHelper.skipBytes(raf, toSkip);
            return true;
        } else {
            return false;
        }
    }

    private byte[] readLocationData(Location location, RandomAccessFile raf) throws IOException {
        if (location.isBatchControlRecord()) {
            byte[] checksum = new byte[Journal.CHECKSUM_SIZE];
            raf.read(checksum);
            return checksum;
        } else {
            byte[] data = new byte[location.getSize() - Journal.RECORD_HEADER_SIZE];
            raf.readFully(data);
            return data;
        }
    }

    private RandomAccessFile getOrCreateRaf(Thread thread, Integer file) throws CompactedDataFileException, IOException {
        ConcurrentMap<Integer, RandomAccessFile> rafs = perThreadDataFileRafs.get(thread);
        if (rafs == null) {
            rafs = new ConcurrentHashMap<Integer, RandomAccessFile>();
            perThreadDataFileRafs.put(thread, rafs);
        }
        RandomAccessFile raf = rafs.get(file);
        if (raf == null) {
            raf = journal.getDataFile(file).openRandomAccessFile();
            IOHelper.skipBytes(raf, journal.FILE_HEADER_SIZE);
            rafs.put(file, raf);
        }
        return raf;
    }

    private void removeRaf(Thread thread, Integer file) throws IOException {
        RandomAccessFile raf = perThreadDataFileRafs.get(thread).remove(file);
        raf.close();
    }

    private Lock getOrCreateLock(Thread thread, Integer file) {
        ConcurrentMap<Integer, Lock> locks = perThreadDataFileLocks.get(thread);
        if (locks == null) {
            locks = new ConcurrentHashMap<Integer, Lock>();
            perThreadDataFileLocks.put(thread, locks);
        }
        Lock lock = locks.get(file);
        if (lock == null) {
            lock = new ReentrantLock();
            locks.put(file, lock);
        }
        return lock;
    }

    private boolean hasRecordHeader(RandomAccessFile raf, long position) throws IOException {
        long remaining = raf.length() - position;
        if (remaining >= Journal.RECORD_HEADER_SIZE) {
            return true;
        } else if (remaining == 0) {
            return false;
        } else {
            throw new IllegalStateException("Remaining file length doesn't fit a record header!");
        }
    }

    private class ResourceDisposer implements Runnable {

        public void run() {
            Set<Thread> deadThreads = new HashSet<Thread>();
            for (Entry<Thread, ConcurrentMap<Integer, RandomAccessFile>> threadRafs : perThreadDataFileRafs.entrySet()) {
                for (Entry<Integer, RandomAccessFile> raf : threadRafs.getValue().entrySet()) {
                    Lock lock = getOrCreateLock(threadRafs.getKey(), raf.getKey());
                    if (lock.tryLock()) {
                        try {
                            removeRaf(threadRafs.getKey(), raf.getKey());
                            if (!threadRafs.getKey().isAlive()) {
                                deadThreads.add(threadRafs.getKey());
                            }
                        } catch (IOException ex) {
                            warn(ex, ex.getMessage());
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
            for (Thread deadThread : deadThreads) {
                perThreadDataFileRafs.remove(deadThread);
                perThreadDataFileLocks.remove(deadThread);
            }
        }
    }
}
