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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ReadWriteTest extends AbstractJournalTest {

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
    
    @Test(expected = IOException.class)
    public void testCannotReadDeletedLocation() throws Exception {
        Location location = journal.write("DATA".getBytes("UTF-8"), Journal.WriteType.ASYNC);
        journal.delete(location);
        journal.read(location, Journal.ReadType.ASYNC);
        fail("Should have raised IOException!");
    }

    @Override
    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024 * 100);
        journal.setMaxWriteBatchSize(1024);
    }
}
