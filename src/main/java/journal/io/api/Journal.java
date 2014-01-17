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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import journal.io.util.IOHelper;
import static journal.io.util.LogHelper.*;

/**
 * Journal implementation based on append-only rotating logs and checksummed
 * records, with concurrent writes and reads, dynamic batching and logs
 * compaction. <br/><br/> Journal records can be written, read and deleted by
 * providing a {@link Location} object. <br/><br/> The whole Journal can be
 * replayed forward or backward by simply obtaining a redo or undo iterable and
 * going through it in a for-each block. <br/>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class Journal {

    static final byte[] MAGIC_STRING = "J.IO".getBytes(Charset.forName("UTF-8"));
    static final int MAGIC_SIZE = MAGIC_STRING.length;
    static final int STORAGE_VERSION = 130;
    static final int STORAGE_VERSION_SIZE = 4;
    static final int FILE_HEADER_SIZE = MAGIC_SIZE + STORAGE_VERSION_SIZE;
    //
    static final int RECORD_POINTER_SIZE = 4;
    static final int RECORD_LENGTH_SIZE = 4;
    static final int RECORD_TYPE_SIZE = 1;
    static final int RECORD_HEADER_SIZE = RECORD_POINTER_SIZE + RECORD_LENGTH_SIZE + RECORD_TYPE_SIZE;
    //
    static final int CHECKSUM_SIZE = 8;
    static final int BATCH_CONTROL_RECORD_SIZE = RECORD_HEADER_SIZE + CHECKSUM_SIZE;
    //
    static final String WRITER_THREAD_GROUP = "Journal.IO - Writer Thread Group";
    static final String WRITER_THREAD = "Journal.IO - Writer Thread";
    static final String DISPOSER_THREAD_GROUP = "Journal.IO - Disposer Thread Group";
    static final String DISPOSER_THREAD = "Journal.IO - Disposer Thread";
    //
    static final int PRE_START_POINTER = -1;
    //
    static final String DEFAULT_DIRECTORY = ".";
    static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    static final String DEFAULT_FILE_PREFIX = "db-";
    static final String DEFAULT_FILE_SUFFIX = ".log";
    static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    static final int DEFAULT_DISPOSE_INTERVAL = 1000 * 60 * 10;
    static final int MIN_FILE_LENGTH = 1024;
    static final int DEFAULT_MAX_BATCH_SIZE = DEFAULT_MAX_FILE_LENGTH;
    //
    private final ConcurrentNavigableMap<Integer, DataFile> dataFiles = new ConcurrentSkipListMap<Integer, DataFile>();
    private final ConcurrentNavigableMap<Location, Long> hints = new ConcurrentSkipListMap<Location, Long>();
    private final ConcurrentMap<Location, WriteCommand> inflightWrites = new ConcurrentHashMap<Location, WriteCommand>();
    private final AtomicLong totalLength = new AtomicLong();
    //
    private volatile Location lastAppendLocation;
    //
    private volatile File directory = new File(DEFAULT_DIRECTORY);
    private volatile File directoryArchive = new File(DEFAULT_ARCHIVE_DIRECTORY);
    //
    private volatile String filePrefix = DEFAULT_FILE_PREFIX;
    private volatile String fileSuffix = DEFAULT_FILE_SUFFIX;
    private volatile int maxWriteBatchSize = DEFAULT_MAX_BATCH_SIZE;
    private volatile int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    private volatile long disposeInterval = DEFAULT_DISPOSE_INTERVAL;
    private volatile boolean physicalSync = false;
    private volatile boolean checksum = true;
    //
    private volatile boolean managedWriter;
    private volatile boolean managedDisposer;
    private volatile Executor writer;
    private volatile ScheduledExecutorService disposer;
    private volatile DataFileAppender appender;
    private volatile DataFileAccessor accessor;
    //
    private volatile boolean opened;
    //
    private volatile boolean archiveFiles;
    //
    private RecoveryErrorHandler recoveryErrorHandler;
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
        if (writer == null) {
            managedWriter = true;
            writer = Executors.newSingleThreadExecutor(new JournalThreadFactory(WRITER_THREAD_GROUP, WRITER_THREAD));
        }
        if (disposer == null) {
            managedDisposer = true;
            disposer = Executors.newSingleThreadScheduledExecutor(new JournalThreadFactory(DISPOSER_THREAD_GROUP, DISPOSER_THREAD));
        }

        if (recoveryErrorHandler == null) {
            recoveryErrorHandler = RecoveryErrorHandler.ABORT;
        }

        opened = true;

        accessor = new DataFileAccessor(this);
        accessor.open();

        appender = new DataFileAppender(this);
        appender.open();

        dataFiles.clear();

        File[] files = directory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix);
            }
        });
        if (files == null) {
            throw new IOException("Failed to access content of " + directory);
        }

        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                String name1 = f1.getName();
                int index1 = Integer.parseInt(name1.substring(filePrefix.length(), name1.length() - fileSuffix.length()));
                String name2 = f2.getName();
                int index2 = Integer.parseInt(name2.substring(filePrefix.length(), name2.length() - fileSuffix.length()));
                return index1 - index2;
            }
        });
        if (files.length > 0) {
            for (int i = 0; i < files.length; i++) {
                try {
                    File file = files[i];
                    String name = file.getName();
                    int index = Integer.parseInt(name.substring(filePrefix.length(), name.length() - fileSuffix.length()));
                    DataFile dataFile = new DataFile(file, index);
                    if (!dataFiles.isEmpty()) {
                        dataFiles.lastEntry().getValue().setNext(dataFile);
                    }
                    dataFiles.put(dataFile.getDataFileId(), dataFile);
                    totalLength.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }
            lastAppendLocation = recoveryCheck();
        }
        if (lastAppendLocation == null) {
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
        //
        opened = false;
        accessor.close();
        appender.close();
        hints.clear();
        inflightWrites.clear();
        //
        if (managedWriter) {
            ((ExecutorService) writer).shutdown();
            writer = null;
        }
        if (managedDisposer) {
            disposer.shutdown();
            disposer = null;
        }
    }

    /**
     * Compact the journal, reducing size of logs containing deleted entries and
     * completely removing empty (with only deleted entries) logs.
     *
     * @throws IOException
     * @throws ClosedJournalException
     */
    public synchronized void compact() throws ClosedJournalException, IOException {
        if (opened) {
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
        } else {
            throw new ClosedJournalException("The journal is closed!");
        }
    }

    /**
     * Sync asynchronously written records on disk.
     *
     * @throws IOException
     * @throws ClosedJournalException
     */
    public void sync() throws ClosedJournalException, IOException {
        try {
            appender.sync().get();
            if (appender.getAsyncException() != null) {
                throw new IOException(appender.getAsyncException());
            }
        } catch (Exception ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }

    /**
     * Truncate the journal, removing all log files. Please note truncate
     * requires the journal to be closed.
     *
     * @throws IOException
     * @throws OpenJournalException
     */
    public synchronized void truncate() throws OpenJournalException, IOException {
        if (!opened) {
            for (DataFile file : dataFiles.values()) {
                removeDataFile(file);
            }
        } else {
            throw new OpenJournalException("The journal is open! The journal must be closed to be truncated.");
        }
    }

    /**
     * Read the record stored at the given {@link Location}, either by syncing
     * with the disk state (if {@code ReadType.SYNC}) or by taking advantage of
     * speculative disk reads (if {@code ReadType.ASYNC}); the latter is faster,
     * while the former is slower but will suddenly detect deleted records.
     *
     * @param location
     * @param read
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     * @throws CompactedDataFileException
     */
    public byte[] read(Location location, ReadType read) throws ClosedJournalException, CompactedDataFileException, IOException {
        return accessor.readLocation(location, read.equals(ReadType.SYNC) ? true : false);
    }

    /**
     * Write the given byte buffer record, either sync (if
     * {@code WriteType.SYNC}) or async (if {@code WriteType.ASYNC}), and
     * returns the stored {@link Location}.<br/> A sync write causes all
     * previously batched async writes to be synced too.
     *
     * @param data
     * @param write
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     */
    public Location write(byte[] data, WriteType write) throws ClosedJournalException, IOException {
        return write(data, write, Location.NoWriteCallback.INSTANCE);
    }

    /**
     * Write the given byte buffer record, either sync (if
     * {@code WriteType.SYNC}) or async (if {@code WriteType.ASYNC}), and
     * returns the stored {@link Location}.<br/> A sync write causes all
     * previously batched async writes to be synced too.<br/> The provided
     * callback will be invoked if sync is completed or if some error occurs
     * during syncing.
     *
     * @param data
     * @param write
     * @param callback
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     */
    public Location write(byte[] data, WriteType write, WriteCallback callback) throws ClosedJournalException, IOException {
        Location loc = appender.storeItem(data, Location.USER_RECORD_TYPE, write.equals(WriteType.SYNC) ? true : false, callback);
        return loc;
    }

    /**
     * Delete the record at the given {@link Location}.<br/> Deletes cause first
     * a batch sync and always are logical: records will be actually deleted at
     * log cleanup time.
     *
     * @param location
     * @throws IOException
     * @throws ClosedJournalException
     * @throws CompactedDataFileException
     */
    public void delete(Location location) throws ClosedJournalException, CompactedDataFileException, IOException {
        accessor.updateLocation(location, Location.DELETED_RECORD_TYPE, physicalSync);
    }

    /**
     * Return an iterable to replay the journal by going through all records
     * locations.
     *
     * @return
     * @throws IOException
     * @throws ClosedJournalException
     * @throws CompactedDataFileException
     */
    public Iterable<Location> redo() throws ClosedJournalException, CompactedDataFileException, IOException {
        Entry<Integer, DataFile> firstEntry = dataFiles.firstEntry();
        if (firstEntry == null) {
            return new Redo(null);
        }
        return new Redo(goToFirstLocation(firstEntry.getValue(), Location.USER_RECORD_TYPE, true));
    }

    /**
     * Return an iterable to replay the journal by going through all records
     * locations starting from the given one.
     *
     * @param start
     * @return
     * @throws IOException
     * @throws CompactedDataFileException
     * @throws ClosedJournalException
     */
    public Iterable<Location> redo(Location start) throws ClosedJournalException, CompactedDataFileException, IOException {
        return new Redo(start);
    }

    /**
     * Return an iterable to replay the journal in reverse, starting with the
     * newest location and ending with the first. The iterable does not include
     * future writes - writes that happen after its creation.
     *
     * @return
     * @throws IOException
     * @throws CompactedDataFileException
     * @throws ClosedJournalException
     */
    public Iterable<Location> undo() throws ClosedJournalException, CompactedDataFileException, IOException {
        return new Undo(redo());
    }

    /**
     * Return an iterable to replay the journal in reverse, starting with the
     * newest location and ending with the specified end location. The iterable
     * does not include future writes - writes that happen after its creation.
     *
     * @param end
     * @return
     * @throws IOException
     * @throws CompactedDataFileException
     * @throws ClosedJournalException
     */
    public Iterable<Location> undo(Location end) throws ClosedJournalException, CompactedDataFileException, IOException {
        return new Undo(redo(end));
    }

    /**
     * Get the files part of this journal.
     *
     * @return
     */
    public List<File> getFiles() {
        List<File> result = new LinkedList<File>();
        for (DataFile dataFile : dataFiles.values()) {
            result.add(dataFile.getFile());
        }
        return result;
    }

    /**
     * Get the max length of each log file.
     *
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
     *
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
     *
     * @return
     */
    public String getFilePrefix() {
        return filePrefix;
    }

    /**
     * Set the prefix for log files.
     *
     * @param filePrefix
     */
    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    /**
     * Get the optional archive directory used to archive cleaned up log files.
     *
     * @return
     */
    public File getDirectoryArchive() {
        return directoryArchive;
    }

    /**
     * Set the optional archive directory used to archive cleaned up log files.
     *
     * @param directoryArchive
     */
    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }

    /**
     * Return true if cleaned up log files should be archived, false otherwise.
     *
     * @return
     */
    public boolean isArchiveFiles() {
        return archiveFiles;
    }

    /**
     * Set true if cleaned up log files should be archived, false otherwise.
     *
     * @param archiveFiles
     */
    public void setArchiveFiles(boolean archiveFiles) {
        this.archiveFiles = archiveFiles;
    }

    /**
     * Set the {@link ReplicationTarget} to replicate batch writes to.
     *
     * @param replicationTarget
     */
    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    /**
     * Get the {@link ReplicationTarget} to replicate batch writes to.
     *
     * @return
     */
    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    /**
     * Get the suffix for log files.
     *
     * @return
     */
    public String getFileSuffix() {
        return fileSuffix;
    }

    /**
     * Set the suffix for log files.
     *
     * @param fileSuffix
     */
    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    /**
     * Return true if records checksum is enabled, false otherwise.
     *
     * @return
     */
    public boolean isChecksum() {
        return checksum;
    }

    /**
     * Set true if records checksum is enabled, false otherwise.
     *
     * @param checksumWrites
     */
    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    /**
     * Return true if every disk write is followed by a physical disk sync,
     * synchronizing file descriptor properties and flushing hardware buffers,
     * false otherwise.
     *
     * @return
     */
    public boolean isPhysicalSync() {
        return physicalSync;
    }

    /**
     * Set true if every disk write must be followed by a physical disk sync,
     * synchronizing file descriptor properties and flushing hardware buffers,
     * false otherwise.
     *
     * @return
     */
    public void setPhysicalSync(boolean physicalSync) {
        this.physicalSync = physicalSync;
    }

    /**
     * Get the max size in bytes of the write batch: must always be equal or
     * less than the max file length.
     *
     * @return
     */
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    /**
     * Set the max size in bytes of the write batch: must always be equal or
     * less than the max file length.
     *
     * @param maxWriteBatchSize
     */
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    /**
     * Set the milliseconds interval for resources disposal: i.e., un-accessed
     * files will be closed.
     *
     * @param disposeInterval
     */
    public void setDisposeInterval(long disposeInterval) {
        this.disposeInterval = disposeInterval;
    }

    /**
     * Get the milliseconds interval for resources disposal.
     *
     * @return
     */
    public long getDisposeInterval() {
        return disposeInterval;
    }

    /**
     * Set the Executor to use for writing new record entries.
     *
     * Important note: the provided Executor must be manually closed.
     *
     * @param writer
     */
    public void setWriter(Executor writer) {
        this.writer = writer;
        this.managedWriter = false;
    }

    /**
     * Set the ScheduledExecutorService to use for internal resources disposing.
     *
     * Important note: the provided ScheduledExecutorService must be manually
     * closed.
     *
     * @param writer
     */
    public void setDisposer(ScheduledExecutorService disposer) {
        this.disposer = disposer;
        this.managedDisposer = false;
    }

    /**
     * Set the RecoveryErrorHandler to invoke in case of checksum errors.
     *
     * @param recoveryErrorHandler
     */
    public void setRecoveryErrorHandler(RecoveryErrorHandler recoveryErrorHandler) {
        this.recoveryErrorHandler = recoveryErrorHandler;
    }

    public String toString() {
        return directory.toString();
    }

    Executor getWriter() {
        return writer;
    }

    ScheduledExecutorService getDisposer() {
        return disposer;
    }

    ConcurrentNavigableMap<Integer, DataFile> getDataFiles() {
        return dataFiles;
    }

    DataFile getDataFile(Integer id) throws CompactedDataFileException {
        Entry<Integer, DataFile> first = dataFiles.firstEntry();
        if (first != null && first.getKey() <= id) {
            return dataFiles.get(id);
        } else {
            throw new CompactedDataFileException(id);
        }
    }

    ConcurrentNavigableMap<Location, Long> getHints() {
        return hints;
    }

    ConcurrentMap<Location, WriteCommand> getInflightWrites() {
        return inflightWrites;
    }

    DataFile getCurrentWriteDataFile() throws IOException {
        if (dataFiles.isEmpty()) {
            newDataFile();
        }
        return dataFiles.lastEntry().getValue();
    }

    DataFile newDataFile() throws IOException {
        int nextNum = !dataFiles.isEmpty() ? dataFiles.lastEntry().getValue().getDataFileId().intValue() + 1 : 1;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        nextWriteFile.writeHeader();
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

    private Location goToFirstLocation(DataFile file, final byte type, final boolean goToNextFile) throws IOException, IllegalStateException {
        Location start = null;
        while (file != null && start == null) {
            start = accessor.readLocationDetails(file.getDataFileId(), 0);
            file = goToNextFile ? file.getNext() : null;
        }
        if (start != null && (start.getType() == type || type == Location.ANY_RECORD_TYPE)) {
            return start;
        } else if (start != null) {
            return goToNextLocation(start, type, goToNextFile);
        } else {
            return null;
        }
    }

    private Location goToNextLocation(Location start, final byte type, final boolean goToNextFile) throws IOException {
        DataFile currentDataFile = dataFiles.get(start.getDataFileId());
        Location currentLocation = new Location(start);
        Location result = null;
        while (result == null) {
            if (currentLocation != null) {
                currentLocation = accessor.readNextLocationDetails(currentLocation, type);
                result = currentLocation;
            } else {
                if (goToNextFile) {
                    currentDataFile = currentDataFile.getNext();
                    if (currentDataFile != null) {
                        currentLocation = accessor.readLocationDetails(currentDataFile.getDataFileId(), 0);
                        if (currentLocation != null && (currentLocation.getType() == type || type == Location.ANY_RECORD_TYPE)) {
                            result = currentLocation;
                        }
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

    private void removeDataFile(DataFile dataFile) throws IOException {
        dataFiles.remove(dataFile.getDataFileId());
        totalLength.addAndGet(-dataFile.getLength());
        Location toRemove = new Location(dataFile.getDataFileId());
        Location candidate = null;
        while ((candidate = hints.higherKey(toRemove)) != null && candidate.getDataFileId() == toRemove.getDataFileId()) {
            hints.remove(candidate);
        }
        accessor.dispose(dataFile);
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
        accessor.pause();
        try {
            DataFile tmpFile = new DataFile(
                    new File(currentFile.getFile().getParent(), filePrefix + currentFile.getDataFileId() + fileSuffix + ".tmp"),
                    currentFile.getDataFileId());
            tmpFile.writeHeader();
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
                Location batchLocation = batch.perform(raf, true, true, null);
                hints.put(batchLocation, batchLocation.getThisFilePosition());
            } finally {
                if (raf != null) {
                    raf.close();
                }
            }
            removeDataFile(currentFile);
            totalLength.addAndGet(tmpFile.getLength());
            IOHelper.renameFile(tmpFile.getFile(), currentFile.getFile());
            dataFiles.put(currentFile.getDataFileId(), currentFile);
            // Increment generation so that sequential reads from locations
            // referring to a different generation will not be valid:
            currentFile.incrementGeneration();
        } finally {
            accessor.resume();
        }
    }

    private Location recoveryCheck() throws IOException {
        List<Location> checksummedLocations = new LinkedList<Location>();
        DataFile currentFile = dataFiles.firstEntry().getValue();
        Location currentBatch = null;
        Location lastBatch = null;
        while (currentFile != null) {
            try {
                currentFile.verifyHeader();
            } catch (IOException ex) {
                DataFile toDelete = currentFile;
                currentFile = toDelete.getNext();
                warn(ex, "Deleting data file: %s", toDelete);
                removeDataFile(toDelete);
                continue;
            }
            try {
                currentBatch = goToFirstLocation(currentFile, Location.BATCH_CONTROL_RECORD_TYPE, false);
                while (currentBatch != null) {
                    try {
                        Location currentLocation = currentBatch;
                        hints.put(currentBatch, currentBatch.getThisFilePosition());
                        if (isChecksum()) {
                            ByteBuffer currentBatchBuffer = ByteBuffer.wrap(accessor.readLocation(currentBatch, false));
                            Checksum actualChecksum = new Adler32();
                            Location nextLocation = goToNextLocation(currentBatch, Location.ANY_RECORD_TYPE, false);
                            long expectedChecksum = currentBatchBuffer.getLong();
                            checksummedLocations.clear();
                            while (nextLocation != null && nextLocation.getType() != Location.BATCH_CONTROL_RECORD_TYPE) {
                                assert currentLocation.compareTo(nextLocation) < 0;
                                byte data[] = accessor.readLocation(nextLocation, false);
                                actualChecksum.update(data, 0, data.length);
                                checksummedLocations.add(nextLocation);
                                currentLocation = nextLocation;
                                nextLocation = goToNextLocation(nextLocation, Location.ANY_RECORD_TYPE, false);
                            }
                            if (checksummedLocations.isEmpty()) {
                                throw new IllegalStateException("Found empty batch!");
                            }
                            if (expectedChecksum != actualChecksum.getValue()) {
                                recoveryErrorHandler.onError(this, checksummedLocations);
                            }
                            if (nextLocation != null) {
                                assert currentLocation.compareTo(nextLocation) < 0;
                            }
                            lastBatch = currentBatch;
                            currentBatch = nextLocation;
                        } else {
                            lastBatch = currentBatch;
                            currentBatch = goToNextLocation(currentBatch, Location.BATCH_CONTROL_RECORD_TYPE, false);
                        }
                    } catch (Throwable ex) {
                        warn(ex, "Corrupted data found, deleting data starting from location %s up to the end of the file.", currentBatch);
                        accessor.deleteFromLocation(currentBatch);
                        break;
                    }
                }
            } catch (Throwable ex) {
                warn(ex, "Corrupted data found while reading first batch location, deleting whole data file: %s", currentFile);
                removeDataFile(currentFile);
            }
            currentFile = currentFile.getNext();
        }
        // Go through records on the last batch to get the last one:
        Location lastRecord = lastBatch;
        while (lastRecord != null) {
            Location next = goToNextLocation(lastRecord, Location.ANY_RECORD_TYPE, false);
            if (next != null) {
                lastRecord = next;
            } else {
                break;
            }
        }
        return lastRecord;
    }

    public static enum ReadType {

        SYNC, ASYNC;
    }

    public static enum WriteType {

        SYNC, ASYNC;
    }

    static class WriteBatch {

        private static byte[] EMPTY_BUFFER = new byte[0];
        //
        private final DataFile dataFile;
        private final Queue<WriteCommand> writes = new ConcurrentLinkedQueue<WriteCommand>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile long offset;
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
            long thisFileLength = offset + thisBatchSize;
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

        Location perform(RandomAccessFile file, boolean checksum, boolean physicalSync, ReplicationTarget replicationTarget) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            Checksum adler32 = new Adler32();
            WriteCommand control = writes.peek();

            // Write an empty batch control record.
            buffer.putInt(control.location.getPointer());
            buffer.putInt(BATCH_CONTROL_RECORD_SIZE);
            buffer.put(Location.BATCH_CONTROL_RECORD_TYPE);
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
                if (checksum) {
                    adler32.update(current.getData(), 0, current.getData().length);
                }
            }

            // Now we can fill in the batch control record properly.
            buffer.position(Journal.RECORD_HEADER_SIZE);
            if (checksum) {
                buffer.putLong(adler32.getValue());
            }

            // Now do the 1 big write.
            file.seek(offset);
            file.write(buffer.array(), 0, size);

            // Then sync:
            if (physicalSync) {
                IOHelper.sync(file.getFD());
            }

            // And replicate:
            try {
                if (replicationTarget != null) {
                    replicationTarget.replicate(control.location, buffer.array());
                }
            } catch (Throwable ex) {
                warn("Cannot replicate!", ex);
            }

            control.location.setThisFilePosition(offset);
            return control.location;
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

    static class WriteCommand {

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
            this.latch = latch != null ? latch : new CountDownLatch(0);
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

    private static class JournalThreadFactory implements ThreadFactory {

        private final String groupName;
        private final String threadName;

        public JournalThreadFactory(String groupName, String threadName) {
            this.groupName = groupName;
            this.threadName = threadName;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(new ThreadGroup(groupName), r, threadName);
        }
    }

    private class Redo implements Iterable<Location> {

        private final Location start;

        public Redo(Location start) {
            this.start = start;
        }

        public Iterator<Location> iterator() {
            return new Iterator<Location>() {
                private Location current = null;
                private Location next = start;

                public boolean hasNext() {
                    return next != null;
                }

                public Location next() {
                    if (next != null) {
                        try {
                            current = next;
                            next = goToNextLocation(current, Location.USER_RECORD_TYPE, true);
                            return current;
                        } catch (IOException ex) {
                            throw new IllegalStateException(ex.getMessage(), ex);
                        }
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                public void remove() {
                    if (current != null) {
                        try {
                            delete(current);
                            current = null;
                        } catch (IOException ex) {
                            throw new IllegalStateException(ex.getMessage(), ex);
                        }
                    } else {
                        throw new IllegalStateException("No location to remove!");
                    }
                }
            };
        }
    }

    private class Undo implements Iterable<Location> {

        private final Object[] stack;
        private final int start;

        public Undo(Iterable<Location> redo) {
            // Object arrays of 12 are about the size of a cache-line (64 bytes)
            // or two, depending on the oops-size.
            Object[] stack = new Object[12];
            // the last element of the arrays refer to the next "fat node."
            // the last element of the last node is null as an end-mark
            int pointer = 10;
            Iterator<Location> itr = redo.iterator();
            while (itr.hasNext()) {
                Location location = itr.next();
                stack[pointer] = location;
                if (pointer == 0) {
                    Object[] tmp = new Object[12];
                    tmp[11] = stack;
                    stack = tmp;
                    pointer = 10;
                } else {
                    pointer--;
                }
            }
            this.start = pointer + 1; // +1 to go back to last write
            this.stack = stack;
        }

        @Override
        public Iterator<Location> iterator() {
            return new Iterator<Location>() {
                private int pointer = start;
                private Object[] ref = stack;
                private Location current;

                @Override
                public boolean hasNext() {
                    return ref[pointer] != null;
                }

                @Override
                public Location next() {
                    Object next = ref[pointer];
                    if (!(ref[pointer] instanceof Location)) {
                        ref = (Object[]) ref[pointer];
                        if (ref == null) {
                            throw new NoSuchElementException();
                        }
                        pointer = 0;
                        return next();
                    }
                    pointer++;
                    return current = (Location) next;
                }

                @Override
                public void remove() {
                    if (current == null) {
                        throw new IllegalStateException("No location to remove!");
                    }
                    try {
                        delete(current);
                        current = null;
                    } catch (IOException e) {
                        throw new IllegalStateException(e.getMessage(), e);
                    }
                }
            };
        }
    }
}
