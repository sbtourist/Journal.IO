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

import journal.io.util.IOHelper;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Sergio Bossa
 */
public class RecoveryTest extends AbstractJournalTest {

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
        try {
            for (int i = 0; i < iterations; i++) {
                journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
            }
        } finally {
            journal.close();
        }

        Journal newJournal = new Journal();
        try {
            newJournal.setDirectory(dir);
            configure(newJournal);
            newJournal.open();
            int i = 0;
            for (Location location : newJournal.redo()) {
                byte[] buffer = newJournal.read(location, Journal.ReadType.ASYNC);
                assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
            }
            assertEquals(iterations, i);
        } finally {
            newJournal.close();
        }
    }

    @Test
    public void testOpenNewJournalInstanceThenRedoAndDeleteData() throws Exception {
        int iterations = 10;
        try {
            for (int i = 0; i < iterations; i++) {
                journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
            }
        } finally {
            journal.close();
        }

        Journal newJournal = new Journal();
        try {
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
        } finally {
            newJournal.close();
        }
    }

    @Test
    public void testOpenNotCompletedJournal() throws Exception {
        int iterations = 10;
        try {
            for (int i = 0; i < iterations; i++) {
                journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
            }
        } finally {
            journal.close();
        }
        deleteFewLastBytes(dir);

        Journal newJournal = new Journal();
        try {
            initJournal(newJournal);
            verifyEqualsData(newJournal, iterations-1);
            newJournal.write(new String("DATA" + (iterations-1)).getBytes("UTF-8"), Journal.WriteType.SYNC);
        } finally {
            newJournal.close();
        }

        newJournal = new Journal();
        try {
            initJournal(newJournal);
            verifyEqualsData(newJournal, iterations);
        } finally {
            newJournal.close();
        }
    }

    private void deleteFewLastBytes(File journalDir) throws Exception {
        RandomAccessFile file = new RandomAccessFile(new File(journalDir, "db-1.log"), "rw");
        try {
            file.setLength(file.length()-1);
        } finally {
            file.close();
        }
    }

    private void initJournal(Journal journal) throws Exception {
        journal.setDirectory(dir);
        configure(journal);
        journal.open();
    }

    private void verifyEqualsData(Journal journal, int expectedCount) throws Exception {
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
        assertEquals(expectedCount, i);
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024 * 100);
        journal.setMaxWriteBatchSize(1024);
        return true;
    }
}
