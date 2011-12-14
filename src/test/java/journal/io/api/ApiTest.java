/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package journal.io.api;

import org.junit.Before;
import java.io.File;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ApiTest {

    private static File JOURNAL_DIR;

    @Test
    public void api() throws Exception {
        // Create ournal and configure some settings:
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
            boolean sync = i % 2 == 0 ? true : false;
            journal.write(new String("DATA" + i).getBytes("UTF-8"), sync);
        }

        // Replay and read the journal:
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] record = journal.read(location);
            assertEquals("DATA" + i++, new String(record, "UTF-8"));
        }

        // Close the journal:
        journal.close();
    }

    @Before
    public void setUp() throws Exception {
        JOURNAL_DIR = File.createTempFile("hawtjournal", "dir", null);
        JOURNAL_DIR.delete();
        JOURNAL_DIR.mkdir();
    }

}
