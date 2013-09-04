/*
 * Copyright 2013 sergio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package journal.io.issues;

import journal.io.api.AbstractJournalTest;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.junit.Test;

public class Issue44Test extends AbstractJournalTest {

    @Test
    public void test() throws Exception {
        int writes = 10;
        byte[] data = "data".getBytes();

        // Write some data:
        for (int i = 0; i < writes; i++) {
            journal.write(data, Journal.WriteType.SYNC);
        }

        // Delet last:
        int toDelete = 1;
        int lastWrite = 10;
        for (Location location : journal.redo()) {
            if (toDelete == lastWrite) {
                journal.delete(location);
            }
            toDelete++;
        }

        // Close:
        journal.close();

        // Open and write again:
        journal.open();
        journal.write(data, Journal.WriteType.SYNC);

        // Close and reopen to kick recovery check and verify internal assertions
        // are verified:
        journal.close();
        journal.open();
    }
}
