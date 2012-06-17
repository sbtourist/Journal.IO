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

import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Sergio Bossa
 */
public class JournalPerformanceTest {

    private Journal journal;
    private File dir;

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
        journal.close();
        //
        System.out.println("Time in millis: " + (System.currentTimeMillis() - start));
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
        journal.close();
        //
        System.out.println("Time in millis: " + (System.currentTimeMillis() - start));
    }

    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024 * 1024 * 32);
        journal.setMaxWriteBatchSize(1024 * 1024);
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
