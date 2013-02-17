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

    @Test
    public void testJournalWithExternalExecutor() throws Exception {
        journal.setWriter(Executors.newFixedThreadPool(10));
        journal.open();
        int iterations = 100000;
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        int i = 0;
        for (Location location : journal.redo()) {
            byte[] buffer = journal.read(location, Journal.ReadType.ASYNC);
            assertEquals("DATA" + i++, new String(buffer, "UTF-8"));
        }
        assertEquals(iterations, i);
    }

    @Test
    public void testJournalWithExternalExecutorAndExecuteWritesWithExecutor() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        journal.setWriter(executor);
        journal.open();

        final byte[] bytes = "a".getBytes();

        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            executor.submit(new Callable<Location>() {
                public Location call() throws IOException {
                    return journal.write(bytes, Journal.WriteType.SYNC);
                }
            }).get(1, TimeUnit.SECONDS);
        }
    }

    @Override
    protected boolean configure(Journal journal) {
        super.configure(journal);
        return false;
    }
}
