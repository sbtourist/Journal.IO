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
package journal.io.issues;

import org.junit.Test;

import java.io.*;
import journal.io.api.AbstractJournalTest;
import journal.io.api.Journal;
import journal.io.api.Location;
import journal.io.api.RecoveryErrorHandler;

import static org.junit.Assert.assertEquals;

/**
 * @author Arek Burdach
 */
public class Issue48Test extends AbstractJournalTest {

    @Test
    public void testWithManyRecordsAndCorruptedData() throws Exception {
        doTest(10, 1);
    }

    @Test
    public void testWithSingleRecordAndCorruptedData() throws Exception {
        doTest(1, 1);
    }

    @Test
    public void testWithManyRecordsAndCorruptedHeader() throws Exception {
        doTest(10, 6);
    }

    @Test
    public void testWithSingleRecordAndCorruptedRecordHeader() throws Exception {
        doTest(1, 6);
    }

    @Test
    public void testWithSingleRecordAndCorruptedBatchHeader() throws Exception {
        doTest(1, 15);
    }

    @Test
    public void testWithSingleRecordAndDeletedBatchHeader() throws Exception {
        doTest(1, 27);
    }

    @Test
    public void testWithOneSpuriousByte() throws Exception {
        doTest(1, -1);
    }

    @Test
    public void testWithManySpuriousBytes() throws Exception {
        doTest(1, -10);
    }

    @Test
    public void testWithZeroedFile() throws Exception {
        doTest(1, Integer.MAX_VALUE);
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024 * 100);
        journal.setMaxWriteBatchSize(1024);
        journal.setRecoveryErrorHandler(RecoveryErrorHandler.DELETE);
        return true;
    }

    private void doTest(int iterations, int bytesToChange) throws Exception {
        try {
            for (int i = 0; i < iterations; i++) {
                journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
            }
        } finally {
            journal.close();
        }

        corrupt(bytesToChange);

        journal.open();
        try {
            verify(iterations - 1);
            journal.write(new String("DATA" + (iterations - 1)).getBytes("UTF-8"), Journal.WriteType.SYNC);
        } finally {
            journal.close();
        }

        journal.open();
        try {
            verify(iterations);
        } finally {
            journal.close();
        }
    }

    private void corrupt(int bytesToChange) throws Exception {
        RandomAccessFile file = new RandomAccessFile(journal.getFiles().get(0), "rw");
        try {
            long newLength = file.length() - bytesToChange;
            file.setLength(newLength > 0 ? newLength : 0);
        } finally {
            file.close();
        }
    }

    private void verify(int expectedCount) throws Exception {
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
        assertEquals(expectedCount, i);
    }
}