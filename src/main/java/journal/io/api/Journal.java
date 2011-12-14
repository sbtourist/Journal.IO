/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package journal.io.api;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Iterator;
import java.util.Set;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import journal.io.util.IOHelper;
import static journal.io.util.LogHelper.*;

/**
 * Journal implementation based on append-only rotating logs and checksummed records, with concurrent writes and reads, 
 * dynamic batching and logs compaction.<br/>
 * Journal records can be written, read and deleted by providing a {@link Location} object.<br/>
 * The whole journal can be replayed by simply iterating through it in a foreach block.<br/>
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class Journal implements Iterable<Location> {

    static final int RECORD_POINTER_SIZE = 4;
    static final int RECORD_LENGTH_SIZE = 4;
    static final int TYPE_SIZE = 1;
    static final int HEADER_SIZE = RECORD_POINTER_SIZE + RECORD_LENGTH_SIZE + TYPE_SIZE;
    //
    static final int BATCH_SIZE = 4;
    static final int CHECKSUM_SIZE = 8;
    static final int BATCH_CONTROL_RECORD_SIZE = HEADER_SIZE + BATCH_SIZE + CHECKSUM_SIZE;
    //
    static final int PRE_START_POINTER = -1;
    //
    static final String DEFAULT_DIRECTORY = ".";
    static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    static final String DEFAULT_FILE_PREFIX = "db-";
    static final String DEFAULT_FILE_SUFFIX = ".log";
    static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    static final int DEFAULT_DISPOSE_INTERVAL = 1000 * 60;
    static final int MIN_FILE_LENGTH = 1024;
    static final int DEFAULT_MAX_BATCH_SIZE = DEFAULT_MAX_FILE_LENGTH;
    //
    private final ConcurrentNavigableMap<Integer, DataFile> dataFiles = new ConcurrentSkipListMap<Integer, DataFile>();
    private final ConcurrentNavigableMap<Location, WriteCommand> inflightWrites = new ConcurrentSkipListMap<Location, WriteCommand>();
    private final AtomicLong totalLength = new AtomicLong();
    private Location lastAppendLocation;
    //
    private File directory = new File(DEFAULT_DIRECTORY);
    private File directoryArchive = new File(DEFAULT_ARCHIVE_DIRECTORY);
    //
    private String filePrefix = DEFAULT_FILE_PREFIX;
    private String fileSuffix = DEFAULT_FILE_SUFFIX;
    private int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;
    private int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    private long disposeInterval = DEFAULT_DISPOSE_INTERVAL;
    private boolean physicalSync = false;
    private boolean checksum = true;
    //
    private DataFileAppender appender;
    private DataFileAccessor accessor;
    //
    private boolean opened;
    //
    private boolean archiveFiles;
    //
    private JournalListener listener;
    //
    private ReplicationTarget replicationTarget;

    /**
     * Open the journal, eventually recovering it if already existent.
     *
     * @throws IOException
     */
    public synchronized void open() throws IOException {
        if (opened) {
            return;
        }

        if (maxFileLength < MIN_FILE_LENGTH) {
            throw new IllegalStateException("Max file length must be equal or greater than: " + MIN_FILE_LENGTH);
        }
        if (maxWriteBatchSize > maxFileLength) {
            throw new IllegalStateException("Max batch size must be equal or less than: " + maxFileLength);
        }

        opened = true;

        accessor = new DataFileAccessor(this);
        accessor.open();

        appender = new DataFileAppender(this);
        appender.open();

        File[] files = directory.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix);
            }

        });
        if (files != null && files.length > 0) {
            for (int i = 0; i < files.length; i++) {
                try {
                    File file = files[i];
                    String n = file.getName();
                    String numStr = n.substring(filePrefix.length(), n.length() - fileSuffix.length());
                    int num = Integer.parseInt(numStr);
                    DataFile dataFile = new DataFile(file, num);
                    dataFiles.put(dataFile.getDataFileId(), dataFile);
                    totalLength.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }
            lastAppendLocation = recoveryCheck();
        } else {
            lastAppendLocation = new Location(1, PRE_START_POINTER);
        }
    }

    /**
     * Close the journal.
     *
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if (!opened) {
            return;
        }
        accessor.close();
        appender.close();
        dataFiles.clear();
        inflightWrites.clear();
        opened = false;
    }

    /**
     * Compact the journal, reducing size of logs containing deleted entries and completely removing completely empty (with only deleted entries) logs.
     *
     * @throws IOException
     */
    public synchronized void compact() throws IOException {
        if (!opened) {
            return;
        } else {
            accessor.pause();
            try {
                for (DataFile file : dataFiles.values()) {
                    // Can't compact the data file (or subsequent files) that is currently being written to:
                    if (file.getDataFileId() >= lastAppendLocation.getDataFileId()) {
                        continue;
                    } else {
                        Location firstUserLocation = goToFirstLocation(file, Location.USER_RECORD_TYPE, false);
                        if (firstUserLocation == null) {
                            removeDataFile(file);
                        } else {
                            Location firstDeletedLocation = goToFirstLocation(file, Location.DELETED_RECORD_TYPE, false);
                            if (firstDeletedLocation != null) {
                                compactDataFile(file, firstUserLocation);
                            }
                        }
                    }
                }
            } finally {
                accessor.resume();
            }
        }
    }

    /**
     * Sync asynchronously written records on disk.
     * 
     * @throws IOException 
     */
    public void sync() throws IOException {
        try {
            appender.sync().get();
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    /**
     * Read the record stored at the given {@link Location}, taking advantage of speculative disk reads.
     *
     * @param location
     * @return
     * @throws IOException
     * @throws IllegalStateException
     */
    public byte[] read(Location location) throws IOException, IllegalStateException {
        return accessor.readLocation(location, false);
    }

    /**
     * Read the record stored at the given {@link Location}, either by syncing with the disk state (if true) or by taking advantage
     * of speculative disk reads (if false); the latter is faster, while the former is slower but will suddenly detect deleted records.
     *
     * @param location
     * @param sync
     * @return
     * @throws IOException
     * @throws IllegalStateException
     */
    public byte[] read(Location location, boolean sync) throws IOException, IllegalStateException {
        return accessor.readLocation(location, sync);
    }

    /**
     * Write the given byte buffer record, either sync or async, and returns the stored {@link Location}.<br/>
     * A sync write causes all previously batched async writes to be synced too.
     *
     * @param data
     * @param sync True if sync, false if async.
     * @return
     * @throws IOException
     * @throws IllegalStateException
     */
    public Location write(byte[] data, boolean sync) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_RECORD_TYPE, sync);
        return loc;
    }

    /**
     * Delete the record at the given {@link Location}.<br/>
     * Deletes cause first a batch sync and always are logical: records will be actually deleted at log cleanup time.
     * @param location
     * @throws IOException
     * @throws IllegalStateException
     */
    public void delete(Location location) throws IOException, IllegalStateException {
        accessor.updateLocation(location, Location.DELETED_RECORD_TYPE, true);
    }

    /**
     * Return an iterator to replay the journal by going through all records locations.
     *
     * @return
     */
    public Iterator<Location> iterator() {
        return new Iterator<Location>() {

            private Location next = init();

            public boolean hasNext() {
                return next != null;
            }

            public Location next() {
                if (next != null) {
                    try {
                        Location current = next;
                        next = goToNextLocation(current, Location.USER_RECORD_TYPE, true);
                        return current;
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex.getMessage(), ex);
                    }
                } else {
                    throw new IllegalStateException("No next location!");
                }
            }

            public void remove() {
                if (next != null) {
                    try {
                        delete(next);
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex.getMessage(), ex);
                    }
                } else {
                    throw new IllegalStateException("No location to remove!");
                }
            }

            private Location init() {
                try {
                    return goToFirstLocation(dataFiles.firstEntry().getValue(), Location.USER_RECORD_TYPE, true);
                } catch (IOException ex) {
                    throw new IllegalStateException(ex.getMessage(), ex);
                }
            }

        };
    }

    /**
     * Get the files part of this journal.
     * @return
     */
    public Set<File> getFiles() {
        Set<File> result = new HashSet<File>();
        for (DataFile dataFile : dataFiles.values()) {
            result.add(dataFile.getFile());
        }
        return result;
    }

    /**
     * Get the max length of each log file.
     * @return
     */
    public int getMaxFileLength() {
        return maxFileLength;
    }

    /**
     * Set the max length of each log file.
     */
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    /**
     * Get the journal directory containing log files.
     * @return
     */
    public File getDirectory() {
        return directory;
    }

    /**
     * Set the journal directory containing log files.
     */
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    /**
     * Get the prefix for log files.
     * @return
     */
    public String getFilePrefix() {
        return filePrefix;
    }

    /**
     * Set the prefix for log files.
     * @param filePrefix
     */
    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    /**
     * Get the optional archive directory used to archive cleaned up log files.
     * @return
     */
    public File getDirectoryArchive() {
        return directoryArchive;
    }

    /**
     * Set the optional archive directory used to archive cleaned up log files.
     * @param directoryArchive
     */
    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }

    /**
     * Return true if cleaned up log files should be archived, false otherwise.
     * @return
     */
    public boolean isArchiveFiles() {
        return archiveFiles;
    }

    /**
     * Set true if cleaned up log files should be archived, false otherwise.
     * @param archiveFiles
     */
    public void setArchiveFiles(boolean archiveFiles) {
        this.archiveFiles = archiveFiles;
    }

    /**
     * Set the {@link ReplicationTarget} to replicate batch writes to.
     * @param replicationTarget
     */
    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    /**
     * Get the {@link ReplicationTarget} to replicate batch writes to.
     * @return
     */
    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    /**
     * Get the suffix for log files.
     * @return
     */
    public String getFileSuffix() {
        return fileSuffix;
    }

    /**
     * Set the suffix for log files.
     * @param fileSuffix
     */
    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    /**
     * Return true if records checksum is enabled, false otherwise.
     * @return
     */
    public boolean isChecksum() {
        return checksum;
    }

    /**
     * Set true if records checksum is enabled, false otherwise.
     * @param checksumWrites
     */
    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    /**
     * Return true if every disk write is followed by a physical disk sync, synchronizing file descriptor properties and flushing hardware buffers,
     * false otherwise.
     * @return 
     */
    public boolean isPhysicalSync() {
        return physicalSync;
    }

    /**
     * Set true if every disk write must be followed by a physical disk sync, synchronizing file descriptor properties and flushing hardware buffers,
     * false otherwise.
     * @return 
     */
    public void setPhysicalSync(boolean physicalSync) {
        this.physicalSync = physicalSync;
    }

    /**
     * Get the max size in bytes of the write batch: must always be equal or less than the max file length.
     * @return
     */
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    /**
     * Set the max size in bytes of the write batch: must always be equal or less than the max file length.
     * @param maxWriteBatchSize
     */
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    /**
     * Get the {@link JournalListener} to notify when syncing batches.
     * @return
     */
    public JournalListener getListener() {
        return listener;
    }

    /**
     * Set the {@link JournalListener} to notify when syncing batches.
     * @param listener
     */
    public void setListener(JournalListener listener) {
        this.listener = listener;
    }

    /**
     * Set the milliseconds interval for resources disposal: i.e., un-accessed files will be closed.
     * @param disposeInterval 
     */
    public void setDisposeInterval(long disposeInterval) {
        this.disposeInterval = disposeInterval;
    }

    /**
     * Get the milliseconds interval for resources disposal.
     * @return 
     */
    public long getDisposeInterval() {
        return disposeInterval;
    }

    public String toString() {
        return directory.toString();
    }

    ConcurrentNavigableMap<Integer, DataFile> getDataFiles() {
        return dataFiles;
    }

    ConcurrentNavigableMap<Location, WriteCommand> getInflightWrites() {
        return inflightWrites;
    }

    DataFile getCurrentWriteFile() throws IOException {
        if (dataFiles.isEmpty()) {
            rotateWriteFile();
        }
        return dataFiles.lastEntry().getValue();
    }

    DataFile rotateWriteFile() {
        int nextNum = !dataFiles.isEmpty() ? dataFiles.lastEntry().getValue().getDataFileId().intValue() + 1 : 1;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        if (!dataFiles.isEmpty()) {
            dataFiles.lastEntry().getValue().setNext(nextWriteFile);
        }
        dataFiles.put(nextWriteFile.getDataFileId(), nextWriteFile);
        return nextWriteFile;
    }

    Location getLastAppendLocation() {
        return lastAppendLocation;
    }

    void setLastAppendLocation(Location location) {
        this.lastAppendLocation = location;
    }

    void addToTotalLength(int size) {
        totalLength.addAndGet(size);
    }

    private Location goToFirstLocation(DataFile file, byte type, boolean goToNextFile) throws IOException, IllegalStateException {
        Location start = accessor.readLocationDetails(file.getDataFileId(), 0);
        if (start != null && start.getType() == type) {
            return start;
        } else if (start != null) {
            return goToNextLocation(start, type, goToNextFile);
        } else {
            return null;
        }
    }

    private Location goToNextLocation(Location start, byte type, boolean goToNextFile) throws IOException {
        DataFile currentDataFile = getDataFile(start);
        Location currentLocation = new Location(start);
        Location result = null;
        while (result == null) {
            currentLocation = accessor.readNextLocationDetails(currentLocation, type);
            if (currentLocation != null) {
                result = currentLocation;
            } else {
                if (goToNextFile) {
                    currentDataFile = currentDataFile.getNext();
                    if (currentDataFile != null) {
                        currentLocation = new Location(currentDataFile.getDataFileId(), 0);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        return result;
    }

    private File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }

    private DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = dataFiles.get(key);
        if (dataFile == null) {
            error("Looking for key %d but not found among data files %s", key, dataFiles);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile;
    }

    private void removeDataFile(DataFile dataFile) throws IOException {
        dataFiles.remove(dataFile.getDataFileId());
        totalLength.addAndGet(-dataFile.getLength());
        if (archiveFiles) {
            dataFile.move(getDirectoryArchive());
        } else {
            boolean deleted = dataFile.delete();
            if (!deleted) {
                warn("Failed to discard data file %s", dataFile.getFile());
            }
        }
    }

    private void compactDataFile(DataFile currentFile, Location firstUserLocation) throws IOException {
        DataFile tmpFile = new DataFile(
                new File(currentFile.getFile().getParent(), filePrefix + currentFile.getDataFileId() + ".tmp" + fileSuffix),
                currentFile.getDataFileId());
        RandomAccessFile raf = tmpFile.openRandomAccessFile();
        try {
            Location currentUserLocation = firstUserLocation;
            WriteBatch batch = new WriteBatch(tmpFile, 0);
            batch.prepareBatch();
            while (currentUserLocation != null) {
                byte[] data = accessor.readLocation(currentUserLocation, false);
                WriteCommand write = new WriteCommand(currentUserLocation, data, true);
                batch.appendBatch(write);
                currentUserLocation = goToNextLocation(currentUserLocation, Location.USER_RECORD_TYPE, false);
            }
            batch.perform(raf, null, null, true, true);
        } finally {
            if (raf != null) {
                raf.close();
            }
        }
        accessor.dispose(currentFile);
        totalLength.addAndGet(-currentFile.getLength());
        totalLength.addAndGet(tmpFile.getLength());
        IOHelper.copyFile(tmpFile.getFile(), currentFile.getFile());
        IOHelper.deleteFile(tmpFile.getFile());
    }

    private Location recoveryCheck() throws IOException {
        Location currentUserBatch = goToFirstLocation(dataFiles.firstEntry().getValue(), Location.BATCH_CONTROL_RECORD_TYPE, false);
        while (true) {
            ByteBuffer buffer = ByteBuffer.wrap(accessor.readLocation(currentUserBatch, false));
            if (isChecksum()) {
                long expectedChecksum = buffer.getLong();
                byte data[] = new byte[buffer.remaining()];
                Checksum checksum = new Adler32();
                buffer.get(data);
                checksum.update(data, 0, data.length);
                if (expectedChecksum != checksum.getValue()) {
                    throw new IOException("Bad checksum for location: " + currentUserBatch);
                }
            }
            Location next = goToNextLocation(currentUserBatch, Location.BATCH_CONTROL_RECORD_TYPE, true);
            if (next != null) {
                currentUserBatch = next;
            } else {
                break;
            }
        }
        Location currentUserRecord = currentUserBatch;
        while (true) {
            Location next = goToNextLocation(currentUserRecord, Location.USER_RECORD_TYPE, false);
            if (next != null) {
                currentUserRecord = next;
            } else {
                break;
            }
        }
        return currentUserRecord;
    }

    static class WriteBatch {

        private static byte[] EMPTY_BUFFER = new byte[0];
        //
        private final DataFile dataFile;
        private final Queue<WriteCommand> writes = new ConcurrentLinkedQueue<WriteCommand>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile int offset;
        private volatile int pointer;
        private volatile int size;

        WriteBatch() {
            this.dataFile = null;
            this.offset = -1;
            this.pointer = -1;
        }

        WriteBatch(DataFile dataFile, int pointer) throws IOException {
            this.dataFile = dataFile;
            this.offset = dataFile.getLength();
            this.pointer = pointer;
            this.size = BATCH_CONTROL_RECORD_SIZE;
        }

        boolean canBatch(WriteCommand write, int maxWriteBatchSize, int maxFileLength) throws IOException {
            int thisBatchSize = size + write.location.getSize();
            int thisFileLength = offset + thisBatchSize;
            if (thisBatchSize > maxWriteBatchSize || thisFileLength > maxFileLength) {
                return false;
            } else {
                return true;
            }
        }

        WriteCommand prepareBatch() throws IOException {
            WriteCommand controlRecord = new WriteCommand(new Location(), EMPTY_BUFFER, false);
            controlRecord.location.setType(Location.BATCH_CONTROL_RECORD_TYPE);
            controlRecord.location.setSize(Journal.BATCH_CONTROL_RECORD_SIZE);
            controlRecord.location.setDataFileId(dataFile.getDataFileId());
            controlRecord.location.setPointer(pointer);
            size = controlRecord.location.getSize();
            dataFile.incrementLength(size);
            writes.offer(controlRecord);
            return controlRecord;
        }

        void appendBatch(WriteCommand writeRecord) throws IOException {
            size += writeRecord.location.getSize();
            dataFile.incrementLength(writeRecord.location.getSize());
            writes.offer(writeRecord);
        }

        void perform(RandomAccessFile file, JournalListener listener, ReplicationTarget replicator, boolean checksum, boolean physicalSync) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            WriteCommand control = writes.peek();

            // Write an empty batch control record.
            buffer.putInt(control.location.getPointer());
            buffer.putInt(BATCH_CONTROL_RECORD_SIZE);
            buffer.put(Location.BATCH_CONTROL_RECORD_TYPE);
            buffer.putInt(0);
            buffer.putLong(0);

            Iterator<WriteCommand> commands = writes.iterator();
            // Skip the control write:
            commands.next();
            // Process others:
            while (commands.hasNext()) {
                WriteCommand current = commands.next();
                buffer.putInt(current.location.getPointer());
                buffer.putInt(current.location.getSize());
                buffer.put(current.location.getType());
                buffer.put(current.getData());
            }

            // Now we can fill in the batch control record properly.
            buffer.position(Journal.HEADER_SIZE);
            buffer.putInt(size - Journal.BATCH_CONTROL_RECORD_SIZE);
            if (checksum) {
                Checksum adler32 = new Adler32();
                adler32.update(buffer.array(),
                        Journal.BATCH_CONTROL_RECORD_SIZE,
                        size - Journal.BATCH_CONTROL_RECORD_SIZE);
                buffer.putLong(adler32.getValue());
            }

            // Now do the 1 big write.
            file.seek(offset);
            file.write(buffer.array(), 0, size);

            if (physicalSync) {
                IOHelper.sync(file.getFD());
            }

            try {
                if (listener != null) {
                    listener.synced(writes.toArray(new WriteCommand[writes.size()]));
                }
            } catch (Throwable ex) {
                warn("Cannot notify listeners!", ex);
            }
            try {
                if (replicator != null) {
                    replicator.replicate(control.location, buffer.array());
                }
            } catch (Throwable ex) {
                warn("Cannot replicate!", ex);
            }
        }

        DataFile getDataFile() {
            return dataFile;
        }

        int getSize() {
            return size;
        }

        CountDownLatch getLatch() {
            return latch;
        }

        Collection<WriteCommand> getWrites() {
            return Collections.unmodifiableCollection(writes);
        }

        boolean isEmpty() {
            return writes.isEmpty();
        }

        int incrementAndGetPointer() {
            return ++pointer;
        }

    }

    static class WriteCommand implements JournalListener.Write {

        private final Location location;
        private final boolean sync;
        private volatile byte[] data;

        WriteCommand(Location location, byte[] data, boolean sync) {
            this.location = location;
            this.data = data;
            this.sync = sync;
        }

        public Location getLocation() {
            return location;
        }

        byte[] getData() {
            return data;
        }

        boolean isSync() {
            return sync;
        }

    }

    static class WriteFuture implements Future<Boolean> {

        private final CountDownLatch latch;

        WriteFuture(CountDownLatch latch) {
            this.latch = latch;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("Cannot cancel this type of future!");
        }

        public boolean isCancelled() {
            throw new UnsupportedOperationException("Cannot cancel this type of future!");
        }

        public boolean isDone() {
            return latch.getCount() == 0;
        }

        public Boolean get() throws InterruptedException, ExecutionException {
            latch.await();
            return true;
        }

        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            boolean success = latch.await(timeout, unit);
            return success;
        }

    }
}
