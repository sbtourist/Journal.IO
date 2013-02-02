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
public class CompactionTest extends AbstractJournalTest {

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

    @Override
    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024 * 100);
        journal.setMaxWriteBatchSize(1024);
    }
}
