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
import java.nio.charset.Charset;
import journal.io.api.AbstractJournalTest;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;

/**
 * @author Sergio Bossa
 */
@Ignore(value = "Ignored to keep the unit tests run as fast as possible.")
public class PerformanceTest extends AbstractJournalTest {

    @Rule
    public final TestRule benchmarkRun = new BenchmarkRule();
    //
    private final int inserts = 1000000;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        byte[] data = "DATA".getBytes(Charset.forName("UTF-8"));
        for (int i = 0; i < inserts; i++) {
            journal.write(data, Journal.WriteType.ASYNC);
        }
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
    public void testRead() throws Exception {
        int counter = 0;
        for (Location location : journal.redo()) {
            journal.read(location, Journal.ReadType.ASYNC);
            counter++;
        }
        assertEquals(inserts, counter);
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024 * 1024 * 16);
        journal.setMaxWriteBatchSize(1024 * 1024 * 2);
        return true;
    }
}
