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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 * @author Sergio Bossa
 */
@Ignore("Takes a long time, so disabled by default.")
public class PerformanceTest {

    private volatile Journal journal;
    private volatile File dir;

    @Before
    public void setUp() throws Exception {
        dir = new File("target/tests/JournalPerformanceTest");
        if (dir.exists()) {
            deleteFilesInDirectory(dir);
        } else {
            dir.mkdirs();
        }
        journal = new Journal();
        journal.setDirectory(dir);
        configure(journal);
        //
        journal.open();
        // Make some writes to warmup:
        for (int i = 0; i < 1000; i++) {
            journal.write(new byte[1], Journal.WriteType.ASYNC);
        }
        //
        journal.close();
        //
        deleteFilesInDirectory(dir);
        //
        journal.open();
    }

    @After
    public void tearDown() throws Exception {
        journal.close();
        deleteFilesInDirectory(dir);
        dir.delete();
    }

    @Test
    public void testSyncPerf() throws Exception {
        long start = System.currentTimeMillis();
        //
        int iterations = 1000000;
        byte[] payload = new byte[100];
        for (int i = 0; i < iterations; i++) {
            journal.write(payload, Journal.WriteType.SYNC);
        }
        //
        System.out.println("testSyncPerf time in millis: " + (System.currentTimeMillis() - start));
        //
        journal.close();
    }

    @Test
    public void testAsyncPerf() throws Exception {
        long start = System.currentTimeMillis();
        //
        int iterations = 1000000;
        byte[] payload = new byte[100];
        for (int i = 0; i < iterations; i++) {
            journal.write(payload, Journal.WriteType.ASYNC);
        }
        //
        System.out.println("testAsyncPerf time in millis: " + (System.currentTimeMillis() - start));
        //
        journal.close();
    }

    @Test
    public void testSequentialReadPerf() throws Exception {
        int iterations = 1000000;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC);
        }
        //
        long start = System.currentTimeMillis();
        int i = 0;
        for (Location current : journal.redo()) {
            assertEquals("" + i++, new String(journal.read(current, Journal.ReadType.ASYNC), "UTF-8"));
        }
        //
        System.out.println("testSequentialReadPerf time in millis: " + (System.currentTimeMillis() - start));
        //
        journal.close();
    }

    @Test
    public void testRandomReadPerf() throws Exception {
        int iterations = 1000000;
        List<Location> locations = new ArrayList<Location>();
        for (int i = 0; i < iterations; i++) {
            locations.add(journal.write(new String("" + i).getBytes("UTF-8"), Journal.WriteType.ASYNC));
        }
        //
        Random generator = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            int n = generator.nextInt(iterations);
            Location read = locations.get(n);
            assertEquals("" + n, new String(journal.read(read, Journal.ReadType.ASYNC), "UTF-8"));
        }
        //
        System.out.println("testRandomReadPerf time in millis: " + (System.currentTimeMillis() - start));
        //
        journal.close();
    }

    @Test
    public void testDeletePerf() throws Exception {
        int iterations = 1000000;
        byte[] payload = new byte[100];
        for (int i = 0; i < iterations; i++) {
            journal.write(payload, Journal.WriteType.ASYNC);
        }
        //
        long start = System.currentTimeMillis();
        //
        for (Location location : journal.redo()) {
            journal.delete(location);
        }
        //
        System.out.println("testDeletePerf time in millis: " + (System.currentTimeMillis() - start));
        //
        journal.close();
    }

    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024 * 1024 * 10);
        journal.setMaxWriteBatchSize(1024);
    }

    private void deleteFilesInDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                if (f.isDirectory()) {
                    deleteFilesInDirectory(f);
                }
                f.delete();
            }
        }
    }
}
