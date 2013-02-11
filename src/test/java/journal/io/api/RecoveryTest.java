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

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class RecoveryTest extends AbstractJournalTest {

    private Journal customJournal;
    
    @Override
    protected void postSetUp() throws Exception {
        customJournal = new Journal();
        customJournal.setDirectory(dir);
        configure(customJournal);
    }
    
    @Override
    protected void preTearDown() throws Exception {
        if (customJournal != null) {
            customJournal.close();
        }
    }
    
    @Override
    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024 * 100);
        journal.setMaxWriteBatchSize(1024);
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
    public void testOpenAndRecoveryWithNewJournalInstanceAfterLargeNumberOfWrites() throws Exception {
        int iterations = 100000;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        journal.close();

        customJournal.open();

        int i = 0;
        for (Location location : customJournal.redo()) {
            byte[] buffer = customJournal.read(location, Journal.ReadType.ASYNC);
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

        customJournal.open();

        int i = 0;
        for (Location location : customJournal.redo()) {
            byte[] buffer = customJournal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
            customJournal.delete(location);
        }
        assertEquals(iterations, i);
    }

}
