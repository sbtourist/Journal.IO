/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package journal.io.api;

import java.io.File;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ApiTest {

    private static File JOURNAL_DIR;

    @Test
    public void api() throws Exception {
        // Create journal and configure some settings:
        Journal journal = new Journal();
        journal.setDirectory(JOURNAL_DIR);
        journal.setArchiveFiles(false);
        journal.setChecksum(true);
        journal.setMaxFileLength(1024 * 1024);
        journal.setMaxWriteBatchSize(1024 * 10);

        // Open the journal:
        journal.open();

        // Write to the journal:
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            Journal.WriteType writeType = i % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), writeType);
        }

        // Replay the journal forward by redoing:
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] record = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(record, "UTF-8"));
        }

        // Replay the journal backward by undoing:
        int j = 1000;
        for (Location location : journal.undo()) {
            byte[] record = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + --i, new String(record, "UTF-8"));
        }

        // Close the journal:
        journal.close();
    }

    @Before
    public void setUp() throws Exception {
        JOURNAL_DIR = File.createTempFile("journal", "dir", null);
        JOURNAL_DIR.delete();
        JOURNAL_DIR.mkdir();
    }
}
