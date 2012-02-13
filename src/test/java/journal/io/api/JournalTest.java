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
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class JournalTest {

    protected static final int DEFAULT_MAX_BATCH_SIZE = 1024 * 1024 * 4;
    private Journal journal;
    private File dir;

    @Before
    public void setUp() throws Exception {
        dir = new File("target/tests/JournalTest");
        if (dir.exists()) {
            deleteFilesInDirectory(dir);
        } else {
            dir.mkdirs();
        }
        journal = new Journal();
        journal.setDirectory(dir);
        configure(journal);
        journal.open();
    }

    @After
    public void tearDown() throws Exception {
        journal.close();
        deleteFilesInDirectory(dir);
        dir.delete();
    }

    @Test(expected = IOException.class)
    public void testAsyncSpeculativeReadWorksButSyncReadRaisesException() throws Exception {
        Location data = journal.write(new String("DATA").getBytes("UTF-8"), true);
        journal.delete(data);
        assertEquals("DATA", journal.read(data, false));
        journal.read(data, true);
    }

    @Test
    public void testSyncLogWritingAndReplaying() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), true);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testAsyncLogWritingAndReplaying() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), false);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testRedoOnEmptyJournal() throws Exception {
        Iterator<Location> itr = journal.redo().iterator();
        assertFalse(itr.hasNext());
        try {
            itr.next();
            fail("should have thrown");
        } catch (Exception e) {
            assertTrue(e instanceof NoSuchElementException);
        }
    }

    @Test
    public void testUndoMustTraverseLogInReverseOrder() throws Exception {
        journal.write("DATA1".getBytes("UTF-8"), false);
        journal.write("DATA2".getBytes("UTF-8"), false);
        journal.write("DATA3".getBytes("UTF-8"), false);
        Iterator<Location> itr = journal.undo().iterator();
        assertTrue(itr.hasNext());
        assertEquals("DATA3", new String(journal.read(itr.next(), false), "UTF-8"));
        assertEquals("DATA2", new String(journal.read(itr.next(), false), "UTF-8"));
        assertEquals("DATA1", new String(journal.read(itr.next(), false), "UTF-8"));
        assertFalse(itr.hasNext());
    }

    @Test
    public void testUndoDoesNotTakeNewWritesIntoAccount() throws Exception {
        journal.write("A".getBytes("UTF-8"), false);
        journal.write("B".getBytes("UTF-8"), false);
        Iterable<Location> undo = journal.undo();
        journal.write("C".getBytes("UTF-8"), false);
        Iterator<Location> locs = undo.iterator();
        assertEquals("B", new String(journal.read(locs.next(), false), "UTF-8"));
        assertEquals("A", new String(journal.read(locs.next(), false), "UTF-8"));
        assertFalse(locs.hasNext());
    }

    @Test
    public void testUndoingLargeChunksOfData() throws Exception {
        byte parts = 127;
        for (byte i = 0; i < parts; i++) {
            journal.write(new byte[]{i}, false);
        }
        Iterator<Location> locs = journal.undo().iterator();
        while (parts > 0) {
            Location loc = locs.next();
            assertArrayEquals(new byte[]{--parts}, journal.read(loc));
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testUndoingPastStartWillThrowException() throws Exception {
        journal.write(new byte[]{1}, false);
        Iterator<Location> itr = journal.undo().iterator();
        itr.next();
        itr.next();
    }

    @Test
    public void testUndoingAnEmptyJournal() throws Exception {
        Iterator<Location> itr = journal.undo().iterator();
        assertFalse(itr.hasNext());
        try {
            itr.next();
            fail("should have thrown");
        } catch (Exception e) {
            assertTrue(e instanceof NoSuchElementException);
        }
    }

    @Test
    public void testUndoingFromLastWriteIteratesOneLocation() throws Exception {
        Location loc = journal.write(new byte[]{23}, false);
        Iterator<Location> itr = journal.undo(loc).iterator();
        assertArrayEquals(new byte[]{23}, journal.read(itr.next()));
        assertFalse(itr.hasNext());
    }

    @Test
    public void testUndoIteratorStopsAtEnd() throws Exception {
        journal.write(new byte[]{11}, false);
        Location end = journal.write(new byte[]{12}, false);
        journal.write(new byte[]{13}, false);
        Iterator<Location> itr = journal.undo(end).iterator();
        assertArrayEquals(new byte[]{13}, journal.read(itr.next()));
        assertArrayEquals(new byte[]{12}, journal.read(itr.next()));
        assertFalse(itr.hasNext());
    }

    @Test
    public void testCanDeleteThroughUndo() throws Exception {
        journal.write(new byte[]{11}, false);
        journal.write(new byte[]{12}, false);
        journal.write(new byte[]{13}, false);
        Iterator<Location> itr = journal.undo().iterator();
        itr.next();
        itr.remove();
        itr = journal.undo().iterator();
        assertArrayEquals(new byte[]{12}, journal.read(itr.next()));
        assertArrayEquals(new byte[]{11}, journal.read(itr.next()));
        assertFalse(itr.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testThrowIllegalStateIfTheSameLocationIsRemovedThroughUndoMoreThanOnce() throws Exception {
        journal.write(new byte[]{11}, false);
        journal.write(new byte[]{12}, false);
        Iterator<Location> itr = journal.undo().iterator();
        itr.next();
        itr.remove();
        itr.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testThrowIllegalStateIfCallingRemoveBeforeNext() throws Exception {
        journal.write(new byte[]{11}, false);
        Iterator<Location> itr = journal.undo().iterator();
        itr.remove();
    }

    @Test
    public void testMixedSyncAsyncLogWritingAndReplaying() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testReplayFromLocation() throws Exception {
        int iterations = 10;
        Location latest = null;
        for (int i = 0; i < iterations; i++) {
            latest = journal.write(new String("DATA" + i).getBytes("UTF-8"), true);
        }
        int i = 0;
        for (Location location : journal.redo(latest)) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA9", new String(buffer, "UTF-8"));
            i++;
        }
        assertEquals(1, i);
    }

    @Test
    public void testLogRecovery() throws Exception {
        int iterations = 10;
        //
        for (int i = 0; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        //
        journal.close();
        //
        journal.open();
        //
        for (int i = iterations; i < iterations * 2; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        //
        int index = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA" + index++, new String(buffer, "UTF-8"));
        }
        assertEquals(iterations * 2, index);
    }

    @Test
    public void testLogSpanningMultipleFiles() throws Exception {
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testLogCompaction() throws Exception {
        int iterations = 1000;
        for (int i = 0; i < iterations / 2; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            Location toDelete = journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
            journal.delete(toDelete);
        }
        for (int i = iterations / 2; i < iterations; i++) {
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        //
        int preCleanupFiles = journal.getFiles().size();
        journal.compact();
        assertTrue(journal.getFiles().size() < preCleanupFiles);
        //
        int i = iterations / 2;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, false);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test(expected = IOException.class)
    public void testCannotReadDeletedLocation() throws Exception {
        Location location = journal.write("DATA".getBytes("UTF-8"), false);
        journal.delete(location);
        journal.read(location, false);
        fail("Should have raised IOException!");
    }

    @Test
    public void testSyncAndCallListener() throws Exception {
        final int iterations = 10;
        final CountDownLatch writeLatch = new CountDownLatch(iterations);
        JournalListener listener = new JournalListener() {

            public void synced(Write[] writes) {
                for (int i = 0; i < writes.length; i++) {
                    writeLatch.countDown();
                }
            }
        };
        journal.setListener(listener);
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), false);
        }
        journal.sync();
        assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testSyncAndCallReplicator() throws Exception {
        final int iterations = 3;
        final CountDownLatch writeLatch = new CountDownLatch(1);
        ReplicationTarget replicator = new ReplicationTarget() {

            public void replicate(Location startLocation, byte[] data) {
                if (startLocation.getDataFileId() == 1 && startLocation.getPointer() == 0) {
                    writeLatch.countDown();
                }
            }
        };
        journal.setReplicationTarget(replicator);
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), false);
        }
        journal.sync();
        assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testBatchWriteCompletesAfterClose() throws Exception {
        byte[] data = "DATA".getBytes();
        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(data, false);
        }
        journal.close();
        assertTrue(journal.getInflightWrites().isEmpty());
    }

    @Test
    public void testNoBatchWriteWithSync() throws Exception {
        byte[] data = "DATA".getBytes();
        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(data, true);
            assertTrue(journal.getInflightWrites().isEmpty());
        }
    }

    @Test
    public void testConcurrentWriteAndRead() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(25);
        int iterations = 1000;
        //
        for (int i = 0; i < iterations; i++) {
            final int index = i;
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        boolean sync = index % 2 == 0 ? true : false;
                        String write = new String("DATA" + index);
                        Location location = journal.write(write.getBytes("UTF-8"), sync);
                        String read = new String(journal.read(location, false), "UTF-8");
                        if (read.equals("DATA" + index)) {
                            counter.incrementAndGet();
                        } else {
                            System.out.println(write);
                            System.out.println(read);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(iterations, counter.get());
    }

    @Test
    public void testCompactionDuringConcurrentWriteAndRead() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(25);
        int iterations = 1000;
        //
        for (int i = 0; i < iterations; i++) {
            final int index = i;
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        boolean sync = index % 2 == 0 ? true : false;
                        String write = new String("DATA" + index);
                        Location location = journal.write(write.getBytes("UTF-8"), sync);
                        String read = new String(journal.read(location, false), "UTF-8");
                        if (read.equals("DATA" + index)) {
                            if (index % 4 == 0) {
                                journal.delete(location);
                            }
                            counter.incrementAndGet();
                        } else {
                            System.out.println(write);
                            System.out.println(read);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
        executor.submit(new Runnable() {

            public void run() {
                try {
                    journal.compact();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(iterations, counter.get());
        int locations = 0;
        for (Location current : journal.redo()) {
            locations++;
        }
        assertEquals(iterations - (iterations / 4), locations);
    }

    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024);
        journal.setMaxWriteBatchSize(1024);
    }

    private void deleteFilesInDirectory(File directory) {
        File[] files = directory.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                deleteFilesInDirectory(f);
            }
            f.delete();
        }
    }
}
