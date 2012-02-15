/**
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package journal.io.api;

import journal.io.api.Journal.WriteCommand;
import journal.io.util.IOHelper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static journal.io.util.LogHelper.warn;

/**
 * File reader/updater to randomly access data files, supporting concurrent thread-isolated reads and writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAccessor {

    private final ScheduledExecutorService executorService;
    private final ConcurrentMap<Thread, ConcurrentMap<Integer, RandomAccessFile>> perThreadDataFileRafs = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, RandomAccessFile>>();
    private final ConcurrentMap<Thread, ConcurrentMap<Integer, Lock>> perThreadDataFileLocks = new ConcurrentHashMap<Thread, ConcurrentMap<Integer, Lock>>();
    private final ReadWriteLock compactionLock = new ReentrantReadWriteLock();
    private final Lock accessorLock = compactionLock.readLock();
    private final Lock compactorMutex = compactionLock.writeLock();
    //
    private final Journal journal;
    //
    private ScheduledFuture resourceDisposerFuture;

    public DataFileAccessor(Journal journal, ScheduledExecutorService executorService) {
        this.journal = journal;
        this.executorService = executorService;
    }

    void updateLocation(Location location, byte type, boolean sync) throws IOException {
        Lock threadLock = getOrCreateLock(Thread.currentThread(), location.getDataFileId());
        accessorLock.lock();
        threadLock.lock();
        try {
            journal.sync();
            //
            RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
            if (seekToLocation(raf, location)) {
                raf.skipBytes(Journal.RECORD_POINTER_SIZE + Journal.RECORD_LENGTH_SIZE);
                raf.write(type);
                location.setType(type);
                if (sync) {
                    IOHelper.sync(raf.getFD());
                }
            } else {
                throw new IOException("Cannot find location: " + location);
            }
        } finally {
            threadLock.unlock();
            accessorLock.unlock();
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
                throw new IOException("Invalid location: " + location);
            }
        }
    }

    Location readLocationDetails(int file, int pointer) throws IOException {
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
            accessorLock.lock();
            threadLock.lock();
            try {
                RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), location.getDataFileId());
                if (seekToLocation(raf, location)) {
                    long position = raf.getFilePointer();
                    location.setPointer(raf.readInt());
                    location.setSize(raf.readInt());
                    location.setType(raf.readByte());
                    if (location.getSize() > 0) {
                        location.setData(readLocationData(location, raf));
                        raf.seek(position);
                        return location;
                    } else {
                        raf.seek(position);
                        return null;
                    }
                } else {
                    return null;
                }
            } finally {
                threadLock.unlock();
                accessorLock.unlock();
            }
        }
    }

    Location readNextLocationDetails(Location start, int type) throws IOException {
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
        } else {
            // Else read from file:
            Lock threadLock = getOrCreateLock(Thread.currentThread(), start.getDataFileId());
            accessorLock.lock();
            threadLock.lock();
            try {
                RandomAccessFile raf = getOrCreateRaf(Thread.currentThread(), start.getDataFileId());
                if (seekToLocation(raf, start) && skipToNextLocation(raf)) {
                    Location next = new Location(start.getDataFileId());
                    long position = 0;
                    do {
                        position = raf.getFilePointer();
                        next.setPointer(raf.readInt());
                        next.setSize(raf.readInt());
                        next.setType(raf.readByte());
                        if (next.getType() != type) {
                            raf.skipBytes(next.getSize() - Journal.HEADER_SIZE);
                        } else {
                            break;
                        }
                    } while (raf.length() - raf.getFilePointer() > Journal.HEADER_SIZE);
                    if (next.getType() == type) {
                        next.setData(readLocationData(next, raf));
                        raf.seek(position);
                        return next;
                    } else {
                        raf.seek(position);
                        return null;
                    }
                } else {
                    return null;
                }
            } finally {
                threadLock.unlock();
                accessorLock.unlock();
            }
        }
    }

    void dispose(DataFile dataFile) {
        for (Entry<Thread, ConcurrentMap<Integer, RandomAccessFile>> threadRafs : perThreadDataFileRafs.entrySet()) {
            for (Entry<Integer, RandomAccessFile> raf : threadRafs.getValue().entrySet()) {
                if (raf.getKey().equals(dataFile.getDataFileId())) {
                    Lock lock = getOrCreateLock(threadRafs.getKey(), raf.getKey());
                    lock.lock();
                    try {
                        removeRaf(threadRafs.getKey(), raf.getKey());
                        return;
                    } catch (IOException ex) {
                        warn(ex, ex.getMessage());
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }
    }

    void open() {
        resourceDisposerFuture = executorService.scheduleAtFixedRate(new ResourceDisposer(), journal.getDisposeInterval(), journal.getDisposeInterval(), TimeUnit.MILLISECONDS);
    }

    void close() {
        resourceDisposerFuture.cancel(false);
    }

    void pause() {
        compactorMutex.lock();
    }

    void resume() {
        compactorMutex.unlock();
    }

    private boolean seekToLocation(RandomAccessFile raf, Location destination) throws IOException {
        // First try the next file position:
        long position = raf.getFilePointer();
        int pointer = -1;
        int length = -1;
        int type = -1;
        if (raf.length() - position > Journal.HEADER_SIZE) {
            pointer = raf.readInt();
            length = raf.readInt();
            type = raf.readByte();
        }
        // Else seek from beginning:
        if (pointer != destination.getPointer() || type != destination.getType()) {
            raf.seek(0);
            position = raf.getFilePointer();
            if (raf.length() - position > Journal.HEADER_SIZE) {
                pointer = raf.readInt();
                while (pointer != destination.getPointer()) {
                    length = raf.readInt();
                    raf.skipBytes(length - Journal.RECORD_POINTER_SIZE - Journal.RECORD_LENGTH_SIZE);
                    position = raf.getFilePointer();
                    if (raf.length() - position > Journal.HEADER_SIZE) {
                        pointer = raf.readInt();
                    } else {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        raf.seek(position);
        return true;
    }

    private boolean skipToNextLocation(RandomAccessFile raf) throws IOException {
        if (raf.length() - raf.getFilePointer() > Journal.HEADER_SIZE) {
            raf.skipBytes(Journal.RECORD_POINTER_SIZE);
            raf.skipBytes(raf.readInt() - Journal.RECORD_POINTER_SIZE - Journal.RECORD_LENGTH_SIZE);
            if (raf.length() - raf.getFilePointer() > Journal.HEADER_SIZE) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private byte[] readLocationData(Location location, RandomAccessFile raf) throws IOException {
        if (location.isBatchControlRecord()) {
            int batchSize = raf.readInt();
            byte[] data = new byte[Journal.CHECKSUM_SIZE + batchSize];
            raf.readFully(data);
            return data;
        } else {
            byte[] data = new byte[location.getSize() - Journal.HEADER_SIZE];
            raf.readFully(data);
            return data;
        }
    }

    private RandomAccessFile getOrCreateRaf(Thread thread, Integer file) throws IOException {
        ConcurrentMap<Integer, RandomAccessFile> rafs = perThreadDataFileRafs.get(thread);
        if (rafs == null) {
            rafs = new ConcurrentHashMap<Integer, RandomAccessFile>();
            perThreadDataFileRafs.put(thread, rafs);
        }
        RandomAccessFile raf = rafs.get(file);
        if (raf == null) {
            raf = journal.getDataFiles().get(file).openRandomAccessFile();
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
