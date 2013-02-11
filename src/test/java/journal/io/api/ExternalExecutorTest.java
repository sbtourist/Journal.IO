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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ExternalExecutorTest extends AbstractJournalTest {

    private Journal customJournal;
    
    @Override
    protected void postSetUp() throws Exception {
        customJournal = new Journal();
        customJournal.setDirectory(dir);
        configure(customJournal);
    }
    
    @Override
    protected void preTearDown() throws Exception {
        if (customJournal != null) {
            customJournal.close();
        }
    }
    
    @Test
    public void testJournalWithExternalExecutor() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        customJournal.setWriter(executor);
        customJournal.open();

        int iterations = 100000;
        for (int i = 0; i < iterations; i++) {
            customJournal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }

        int i = 0;
        for (Location location : customJournal.redo()) {
            byte[] buffer = customJournal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }

        assertEquals(iterations, i);
    }

    @Test
    public void testJournalWithExternalExecutorAndExecuteWritesWithExecutor() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        customJournal.setWriter(executor);
        customJournal.open();

        final byte[] bytes = "a".getBytes();

        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            executor.submit(new Callable<Location>() {
                public Location call() throws IOException {
                    return customJournal.write(bytes, Journal.WriteType.SYNC);
                }
            }).get(1, TimeUnit.SECONDS);
        }
    }
}
