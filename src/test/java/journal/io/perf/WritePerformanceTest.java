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
package journal.io.perf;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import journal.io.api.AbstractJournalTest;
import journal.io.api.Journal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestRule;

/**
 * @author Sergio Bossa
 */
@Ignore("Takes a long time, so disabled by default.")
public class WritePerformanceTest extends AbstractJournalTest {

    @Rule
    public final TestRule benchmarkRun = new BenchmarkRule();
    //
    private final int inserts = 1000000;

    @Test
    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
    public void testAsyncWrite() throws Exception {
        byte[] data = new byte[100];
        for (int i = 0; i < inserts; i++) {
            journal.write(data, Journal.WriteType.ASYNC);
        }
        journal.sync();
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
    public void testSyncWrite() throws Exception {
        byte[] data = new byte[100];
        for (int i = 0; i < inserts; i++) {
            journal.write(data, Journal.WriteType.SYNC);
        }
        journal.sync();
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
    public void testConcurrentWrite() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final byte[] data = new byte[100];
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        for (int i = 0; i < inserts; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    try {
                        journal.write(data, Journal.WriteType.ASYNC);
                        counter.incrementAndGet();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(inserts, counter.get());
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024 * 1024 * 16);
        journal.setMaxWriteBatchSize(1024 * 1024 * 2);
        return true;
    }
}