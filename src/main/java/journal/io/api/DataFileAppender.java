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

import journal.io.api.Journal.WriteBatch;
import journal.io.api.Journal.WriteCommand;
import journal.io.api.Journal.WriteFuture;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static journal.io.util.LogHelper.*;

/**
 * File writer to do batch appends to a data file, based on a non-blocking,
 * mostly lock-free, algorithm to maximize throughput on concurrent writes.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFileAppender {

    private final WriteBatch NULL_BATCH = new WriteBatch();
    //
    private final int SPIN_RETRIES = 100;
    private final int SPIN_BACKOFF = 10;
    //
    private final BlockingQueue<WriteBatch> batchQueue = new LinkedBlockingQueue<WriteBatch>();
    private final AtomicReference<Exception> asyncException = new AtomicReference<Exception>();
    private final AtomicBoolean batching = new AtomicBoolean(false);
    private final AtomicBoolean writing = new AtomicBoolean(false);
    //
    private final Journal journal;
    //
    private volatile WriteBatch nextWriteBatch;
    private volatile DataFile lastAppendDataFile;
    private volatile RandomAccessFile lastAppendRaf;
    private volatile Executor writer;
    private volatile boolean running;
    private volatile boolean shutdown;

    DataFileAppender(Journal journal) {
        this.journal = journal;
    }

    Location storeItem(byte[] data, byte type, boolean sync, WriteCallback callback) throws IOException {
        int size = Journal.HEADER_SIZE + data.length;

        Location location = new Location();
        location.setSize(size);
        location.setType(type);
        location.setWriteCallback(callback);
        WriteCommand write = new WriteCommand(location, data, sync);
        location = enqueueBatch(write);

        if (sync) {
            try {
                location.getLatch().await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        return location;
    }

    Future<Boolean> sync() throws IOException {
        int spinnings = 0;
        int limit = SPIN_RETRIES;
        while (true) {
            if (shutdown) {
                throw new IOException("Writer Thread Shutdown!");
            }
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            try {
                if (batching.compareAndSet(false, true)) {
                    try {
                        Future result = null;
                        if (nextWriteBatch != null) {
                            result = new WriteFuture(nextWriteBatch.getLatch());
                            batchQueue.put(nextWriteBatch);
                            signalBatch();
                            nextWriteBatch = null;
                        } else {
                            result = new WriteFuture(journal.getLastAppendLocation().getLatch());
                        }
                        return result;
                    } finally {
                        batching.set(false);
                    }
                } else {
                    // Spin waiting for new batch ...
                    if (spinnings <= limit) {
                        spinnings++;
                        continue;
                    } else {
                        Thread.sleep(SPIN_BACKOFF);
                        continue;
                    }
                }
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
    }

    private Location enqueueBatch(WriteCommand writeRecord) throws IOException {
        WriteBatch currentBatch = null;
        int spinnings = 0;
        int limit = SPIN_RETRIES;
        while (true) {
            if (shutdown) {
                throw new IOException("Writer Thread Shutdown!");
            }
            if (asyncException.get() != null) {
                throw new IOException(asyncException.get());
            }
            try {
                if (!shutdown && batching.compareAndSet(false, true)) {
                    boolean hasNewBatch = false;
                    try {
                        if (nextWriteBatch == null) {
                            DataFile file = journal.getCurrentWriteFile();
                            boolean canBatch = false;
                            currentBatch = new WriteBatch(file, journal.getLastAppendLocation().getPointer() + 1);
                            canBatch = currentBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                            if (!canBatch) {
                                file = journal.rotateWriteFile();
                                currentBatch = new WriteBatch(file, 0);
                            }
                            WriteCommand controlRecord = currentBatch.prepareBatch();
                            writeRecord.getLocation().setDataFileId(file.getDataFileId());
                            writeRecord.getLocation().setPointer(currentBatch.incrementAndGetPointer());
                            writeRecord.getLocation().setLatch(currentBatch.getLatch());
                            currentBatch.appendBatch(writeRecord);
                            if (!writeRecord.isSync()) {
                                journal.getInflightWrites().put(controlRecord.getLocation(), controlRecord);
                                journal.getInflightWrites().put(writeRecord.getLocation(), writeRecord);
                                nextWriteBatch = currentBatch;
                            } else {
                                batchQueue.put(currentBatch);
                                hasNewBatch = true;
                            }
                            journal.setLastAppendLocation(writeRecord.getLocation());
                            break;
                        } else {
                            boolean canBatch = nextWriteBatch.canBatch(writeRecord, journal.getMaxWriteBatchSize(), journal.getMaxFileLength());
                            writeRecord.getLocation().setDataFileId(nextWriteBatch.getDataFile().getDataFileId());
                            writeRecord.getLocation().setPointer(nextWriteBatch.incrementAndGetPointer());
                            writeRecord.getLocation().setLatch(nextWriteBatch.getLatch());
                            if (canBatch && !writeRecord.isSync()) {
                                nextWriteBatch.appendBatch(writeRecord);
                                journal.getInflightWrites().put(writeRecord.getLocation(), writeRecord);
                                journal.setLastAppendLocation(writeRecord.getLocation());
                                break;
                            } else if (canBatch && writeRecord.isSync()) {
                                nextWriteBatch.appendBatch(writeRecord);
                                journal.setLastAppendLocation(writeRecord.getLocation());
                                batchQueue.put(nextWriteBatch);
                                nextWriteBatch = null;
                                hasNewBatch = true;
                                break;
                            } else {
                                batchQueue.put(nextWriteBatch);
                                nextWriteBatch = null;
                                hasNewBatch = true;
                            }
                        }
                    } finally {
                        batching.set(false);
                        if (hasNewBatch) {
                            signalBatch();
                        }
                    }
                } else if (!shutdown) {
                    // Spin waiting for new batch ...
                    if (spinnings <= limit) {
                        spinnings++;
                        continue;
                    } else {
                        Thread.sleep(SPIN_BACKOFF);
                        continue;
                    }
                }
            } catch (InterruptedException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
        return writeRecord.getLocation();
    }

    void open() {
        if (!running) {
            running = true;
            writer = journal.getWriter();
        }
    }

    void close() throws IOException {
        try {
            if (!shutdown) {
                if (running) {
                    shutdown = true;
                    running = false;
                    while (batching.get() == true) {
                        Thread.sleep(SPIN_BACKOFF);
                    }
                    if (nextWriteBatch != null) {
                        batchQueue.put(nextWriteBatch);
                        signalBatch();
                        nextWriteBatch.getLatch().await();
                        nextWriteBatch = null;
                    }
                    journal.setLastAppendLocation(null);
                    if (lastAppendRaf != null) {
                        lastAppendRaf.close();
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    public Exception getAsyncException() {
        return asyncException.get();
    }

    /**
     * Signal writer thread to process batches.
     */
    private void signalBatch() {
        writer.execute(new Runnable() {

            @Override
            public void run() {
                // Wait for other threads writing on the same journal to finish:
                while (writing.compareAndSet(false, true) == false) {
                    try {
                        Thread.sleep(SPIN_BACKOFF);
                    } catch (Exception ex) {
                    }
                }
                // TODO: Improve by employing different spinning strategies?
                WriteBatch wb = batchQueue.poll();
                try {
                    while (!shutdown || wb != null) {
                        if (wb != null && !wb.isEmpty()) {
                            boolean newOrRotated = lastAppendDataFile != wb.getDataFile();
                            if (newOrRotated) {
                                if (lastAppendRaf != null) {
                                    lastAppendRaf.close();
                                }
                                lastAppendDataFile = wb.getDataFile();
                                lastAppendRaf = lastAppendDataFile.openRandomAccessFile();
                            }

                            // Perform batch:
                            wb.perform(lastAppendRaf, journal.isChecksum(), journal.isPhysicalSync(), journal.getReplicationTarget());

                            // Adjust journal length:
                            journal.addToTotalLength(wb.getSize());

                            // Now that the data is on disk, notify callbacks and remove the writes from the in-flight cache:
                            for (WriteCommand current : wb.getWrites()) {
                                try {
                                    current.getLocation().getWriteCallback().onSync(current.getLocation());
                                } catch (Throwable ex) {
                                    warn(ex, ex.getMessage());
                                }
                                journal.getInflightWrites().remove(current.getLocation());
                            }

                            // Finally signal any waiting threads that the write is on disk.
                            wb.getLatch().countDown();
                        }
                        // Poll next batch:
                        wb = batchQueue.poll();
                    }
                } catch (Exception ex) {
                    // Put back latest batch:
                    batchQueue.offer(wb);
                    // Notify error to all locations of all batches, and signal waiting threads:
                    for (WriteBatch currentBatch : batchQueue) {
                        for (WriteCommand currentWrite : currentBatch.getWrites()) {
                            try {
                                currentWrite.getLocation().getWriteCallback().onError(currentWrite.getLocation(), ex);
                            } catch (Throwable innerEx) {
                                warn(innerEx, innerEx.getMessage());
                            }
                        }
                        currentBatch.getLatch().countDown();
                    }
                    // Propagate exception:
                    asyncException.compareAndSet(null, ex);
                } finally {
                    writing.set(false);
                }
            }
        });
    }
}
