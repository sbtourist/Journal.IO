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
package journal.io;

import java.io.File;
import java.nio.file.Files;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ApiTest {

    private static File JOURNAL_DIR;

    @Before
    public void setUp() throws Exception {
        JOURNAL_DIR = Files.createTempDirectory("ApiTest" + "journal").toFile();
    }

    @Test
    public void api() throws Exception {
        // Create journal and configure some settings:
        JournalBuilder builder = JournalBuilder.of(JOURNAL_DIR)
                .setChecksum(true)
                .setMaxFileLength(1024 * 1024)
                .setMaxWriteBatchSize(1024 * 10);
        // Open the journal:
        Journal journal = builder.open();

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

        // Delete locations:
        for (Location location : journal.redo()) {
            journal.delete(location);
        }

        // Compact logs:
        journal.compact();

        // Close the journal:
        journal.close();
    }
}
