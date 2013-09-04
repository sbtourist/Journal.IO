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
public class TruncateTest extends AbstractJournalTest {

    @Test
    public void testLogTruncate() throws Exception {
        // Insert data:
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA1" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        int index1 = 0;
        int total1 = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals(new String("DATA1" + index1++), new String(buffer, "UTF-8"));
            total1++;
        }
        assertEquals(iterations, total1);
        journal.close();

        // Truncate:
        journal.truncate();

        // Reopen and verify truncate removed all log files:
        journal.open();
        assertEquals(0, journal.getDataFiles().size());

        // Insert data again and verify reads:
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA2" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        int index2 = 0;
        int total2 = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals(new String("DATA2" + index2++), new String(buffer, "UTF-8"));
            total2++;
        }
        assertEquals(iterations, total2);
    }

    @Test(expected = OpenJournalException.class)
    public void testCannotTruncateOpenJournal() throws Exception {
        journal.truncate();
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024);
        journal.setMaxWriteBatchSize(500);
        return true;
    }
}
