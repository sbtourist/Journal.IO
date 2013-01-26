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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import journal.io.AbstractJournalTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class JournalTest {

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
        Location data = journal.write(new String("DATA").getBytes("UTF-8"), Journal.WriteType.SYNC);
        journal.delete(data);
        assertEquals("DATA", journal.read(data, Journal.ReadType.ASYNC));
        journal.read(data, Journal.ReadType.SYNC);
    }

    @Test
    public void testSyncLogWritingAndRedoing() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testSyncLogWritingAndUndoing() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        int i = 10;
        for (Location location : journal.undo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + --i, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testAsyncLogWritingAndRedoing() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.SYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testAsyncLogWritingAndUndoing() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
        }
        int i = 10;
        for (Location location : journal.undo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + --i, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testMixedSyncAsyncLogWritingAndRedoing() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testMixedSyncAsyncLogWritingAndUndoing() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        int i = 10;
        for (Location location : journal.undo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + --i, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testRedoForwardOrder() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> redo = journal.redo().iterator();
        assertTrue(redo.hasNext());
        assertEquals("A", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertTrue(redo.hasNext());
        assertEquals("B", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertTrue(redo.hasNext());
        assertEquals("C", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertFalse(redo.hasNext());
    }

    @Test
    public void testRedoForwardOrderWithStartingLocation() throws Exception {
        Location a = journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Location b = journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Location c = journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> redo = journal.redo(b).iterator();
        assertTrue(redo.hasNext());
        assertEquals("B", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertTrue(redo.hasNext());
        assertEquals("C", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertFalse(redo.hasNext());
    }

    @Test
    public void testRedoEmptyJournal() throws Exception {
        int iterations = 0;
        for (Location loc : journal.redo()) {
            iterations++;
        }
        assertEquals(0, iterations);
    }
    
    @Test
    public void testRedoJournalWithOnlyDeletedEntries() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        for (Location location : journal.redo()) {
            journal.delete(location);
        }
        int found = 0;
        for (Location loc : journal.redo()) {
            found++;
        }
        assertEquals(0, found);
    }
    

    @Test
    public void testRedoLargeChunksOfData() throws Exception {
        byte parts = 127;
        for (byte i = 0; i < parts; i++) {
            journal.write(new byte[]{i}, Journal.WriteType.ASYNC);
        }
        parts = 0;
        for (Location loc : journal.redo()) {
            assertArrayEquals(new byte[]{parts++}, journal.read(loc, Journal.ReadType.ASYNC));
        }
        assertEquals(127, parts);
    }

    @Test
    public void testRedoTakesNewWritesIntoAccount() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> redo = journal.redo().iterator();
        journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        assertEquals("A", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertEquals("B", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertEquals("C", new String(journal.read(redo.next(), Journal.ReadType.ASYNC), "UTF-8"));
    }

    @Test
    public void testRemoveThroughRedo() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.redo().iterator();
        int iterations = 0;
        while (itr.hasNext()) {
            itr.next();
            itr.remove();
            iterations++;
        }
        assertEquals(3, iterations);
        iterations = 0;
        for (Location loc : journal.redo()) {
            iterations++;
        }
        assertEquals(0, iterations);
    }

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElementExceptionWithRedoIterator() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.redo().iterator();
        assertTrue(itr.hasNext());
        itr.next();
        assertFalse(itr.hasNext());
        itr.next();
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionIfTheSameLocationIsRemovedThroughRedoMoreThanOnce() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.redo().iterator();
        itr.next();
        itr.remove();
        itr.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionIfCallingRemoveBeforeNextWithRedo() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.redo().iterator();
        itr.remove();
    }

    @Test
    public void testUndoBackwardOrder() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> undo = journal.undo().iterator();
        assertTrue(undo.hasNext());
        assertEquals("C", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertTrue(undo.hasNext());
        assertEquals("B", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertTrue(undo.hasNext());
        assertEquals("A", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertFalse(undo.hasNext());
    }

    @Test
    public void testUndoBackwardOrderWithEndingLocation() throws Exception {
        Location a = journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Location b = journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Location c = journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> undo = journal.undo(b).iterator();
        assertTrue(undo.hasNext());
        assertEquals("C", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertTrue(undo.hasNext());
        assertEquals("B", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertFalse(undo.hasNext());
    }

    @Test
    public void testUndoEmptyJournal() throws Exception {
        int iterations = 0;
        for (Location loc : journal.undo()) {
            iterations++;
        }
        assertEquals(0, iterations);
    }

    @Test
    public void testUndoLargeChunksOfData() throws Exception {
        byte parts = 127;
        for (byte i = 0; i < parts; i++) {
            journal.write(new byte[]{i}, Journal.WriteType.ASYNC);
        }
        parts = 127;
        for (Location loc : journal.undo()) {
            assertArrayEquals(new byte[]{--parts}, journal.read(loc, Journal.ReadType.ASYNC));
        }
        assertEquals(0, parts);
    }

    @Test
    public void testUndoDoesntTakeNewWritesIntoAccount() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> undo = journal.undo().iterator();
        journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        assertEquals("B", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertEquals("A", new String(journal.read(undo.next(), Journal.ReadType.ASYNC), "UTF-8"));
        assertFalse(undo.hasNext());
    }

    @Test
    public void testRemoveThroughUndo() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("B".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.write("C".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.undo().iterator();
        int iterations = 0;
        while (itr.hasNext()) {
            itr.next();
            itr.remove();
            iterations++;
        }
        assertEquals(3, iterations);
        iterations = 0;
        for (Location loc : journal.undo()) {
            iterations++;
        }
        assertEquals(0, iterations);
    }

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElementExceptionWithUndoIterator() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.undo().iterator();
        assertTrue(itr.hasNext());
        itr.next();
        assertFalse(itr.hasNext());
        itr.next();
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionIfTheSameLocationIsRemovedThroughUndoMoreThanOnce() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.undo().iterator();
        itr.next();
        itr.remove();
        itr.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionIfCallingRemoveBeforeNextWithUndo() throws Exception {
        journal.write("A".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        Iterator<Location> itr = journal.undo().iterator();
        itr.remove();
    }

    @Test
    public void testLogRecoveryWithFollowingWrites() throws Exception {
        int iterations = 100;
        //
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        //
        journal.close();
        //
        journal.open();
        //
        for (int i = iterations; i < iterations * 2; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        //
        int index = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + index++, new String(buffer, "UTF-8"));
        }
        assertEquals(iterations * 2, index);
    }

    @Test
    public void testLogRecoveryWithDeletes() throws Exception {
        int iterations = 10;
        //
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            Location written = journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
            journal.delete(written);
        }
        //
        journal.close();
        //
        journal.open();
    }

    @Test
    public void testLogRecoveryWithDeletesAndCompact() throws Exception {
        int iterations = 10;
        //
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            Location written = journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
            journal.delete(written);
        }
        //
        journal.compact();
        //
        journal.close();
        //
        journal.open();
    }

    @Test
    public void testLogSpanningMultipleFiles() throws Exception {
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test
    public void testLogCompaction() throws Exception {
        int iterations = 1000;
        String data = new String(new byte[500], "UTF-8");
        for (int i = 0; i < iterations / 2; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            Location toDelete = journal.write(new String(data + i).getBytes("UTF-8"), sync);
            journal.delete(toDelete);
        }
        for (int i = iterations / 2; i < iterations; i++) {
            Journal.WriteType sync = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String(data + i).getBytes("UTF-8"), sync);
        }
        //
        int preCleanupFiles = journal.getFiles().size();
        journal.compact();
        assertTrue(journal.getFiles().size() < preCleanupFiles);
        //
        int i = iterations / 2;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals(data + i++, new String(buffer, "UTF-8"));
        }
    }

    @Test(expected = IOException.class)
    public void testCannotReadDeletedLocation() throws Exception {
        Location location = journal.write("DATA".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.delete(location);
        journal.read(location, Journal.ReadType.ASYNC);
        fail("Should have raised IOException!");
    }

    @Test
    public void testWriteCallbackOnSync() throws Exception {
        final int iterations = 10;
        final CountDownLatch writeLatch = new CountDownLatch(iterations);
        WriteCallback callback = new WriteCallback() {

            @Override
            public void onSync(Location syncedLocation) {
                writeLatch.countDown();
            }

            @Override
            public void onError(Location location, Throwable error) {
            }
        };
        for (int i = 0; i < iterations; i++) {
            journal.write(new byte[]{(byte) i}, Journal.WriteType.ASYNC, callback);
        }
        journal.sync();
        assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testWriteCallbackOnError() throws Exception {
        final int iterations = 10;
        final CountDownLatch writeLatch = new CountDownLatch(iterations);
        WriteCallback callback = new WriteCallback() {

            @Override
            public void onSync(Location syncedLocation) {
            }

            @Override
            public void onError(Location location, Throwable error) {
                writeLatch.countDown();
            }
        };
        for (int i = 0; i < iterations; i++) {
            journal.write(new byte[]{(byte) i}, Journal.WriteType.ASYNC, callback);
        }
        deleteFilesInDirectory(dir);
        dir.delete();
        try {
            journal.sync();
        } catch (Exception ex) {
        }
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
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
        }
        journal.sync();
        assertTrue(writeLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testBatchWriteCompletesAfterClose() throws Exception {
        byte[] data = "DATA".getBytes();
        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(data, Journal.WriteType.ASYNC);
        }
        journal.close();
        assertTrue(journal.getInflightWrites().isEmpty());
    }

    @Test
    public void testNoBatchWriteWithSync() throws Exception {
        byte[] data = "DATA".getBytes();
        final int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(data, Journal.WriteType.SYNC);
            assertTrue(journal.getInflightWrites().isEmpty());
        }
    }

    @Test
    public void testOpenAndRecoveryWithNewJournalInstanceAfterLargeNumberOfWrites() throws Exception {
        int iterations = 100000;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        journal.close();

        Journal newJournal = new Journal();
        newJournal.setDirectory(dir);
        configure(newJournal);
        newJournal.open();
        int i = 0;
        for (Location location : newJournal.redo()) {
            byte[] buffer = newJournal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
        assertEquals(iterations, i);
    }
    
    @Test
    public void testOpenNewJournalInstanceThenRedoAndDeleteData() throws Exception {
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        journal.close();

        Journal newJournal = new Journal();
        newJournal.setDirectory(dir);
        configure(newJournal);
        newJournal.open();
        int i = 0;
        for (Location location : newJournal.redo()) {
            byte[] buffer = newJournal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
            newJournal.delete(location);
        }
        assertEquals(iterations, i);
    }
    
    @Test
    public void testJournalWithExternalExecutor() throws Exception {
        Journal customJournal = new Journal();
        customJournal.setDirectory(dir);
        customJournal.setWriter(Executors.newFixedThreadPool(10));
        configure(customJournal);
        customJournal.open();
        int iterations = 100000;
        for (int i = 0; i < iterations; i++) {
            customJournal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        int i = 0;
        for (Location location : customJournal.redo()) {
            byte[] buffer = customJournal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
        assertEquals(iterations, i);
    }

    @Test
    public void testJournalWithExternalExecutorAndExecuteWritesWithExecutor() throws Exception {
        final Journal customJournal = new Journal();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        customJournal.setDirectory(dir);
        customJournal.setWriter(executor);
        configure(customJournal);
        customJournal.open();

        final byte[] bytes = "a".getBytes();

        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            executor.submit(new Callable<Location>() {
                public Location call() throws IOException {
                    return customJournal.write(bytes, Journal.WriteType.SYNC);
                }
            }).get(1, TimeUnit.SECONDS);
        }
    }

    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024 * 100);
        journal.setMaxWriteBatchSize(1024);
    }

    private void deleteFilesInDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                if (f.isDirectory()) {
                    deleteFilesInDirectory(f);
                }
                f.delete();
            }
        }
    }
}
