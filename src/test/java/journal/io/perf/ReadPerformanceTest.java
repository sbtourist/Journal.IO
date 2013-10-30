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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
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
@Ignore("Takes a long time, so disabled by default.")
public class ReadPerformanceTest extends AbstractJournalTest {

    @Rule
    public final TestRule benchmarkRun = new BenchmarkRule();
    //
    private final int inserts = 1000000;
    private final List<Location> locations = new CopyOnWriteArrayList<Location>();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        List<Location> localLocations = new ArrayList<Location>(inserts);
        byte[] data = new byte[100];
        for (int i = 0; i < inserts; i++) {
            localLocations.add(journal.write(new String(i + new String(data)).getBytes(Charset.forName("UTF-8")), Journal.WriteType.ASYNC));
        }
        journal.sync();
        locations.clear();
        locations.addAll(localLocations);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
    public void testSequentialRead() throws Exception {
        int counter = 0;
        for (Location location : journal.redo()) {
            String expected = "" + counter;
            assertEquals(expected, new String(journal.read(location, Journal.ReadType.ASYNC), "UTF-8").substring(0, expected.length()));
            counter++;
        }
        assertEquals(inserts, counter);
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 3, warmupRounds = 1)
    public void testRandomRead() throws Exception {
        Random generator = new Random();
        for (int i = 0; i < 1000; i++) {
            int n = generator.nextInt(inserts);
            Location read = locations.get(n);
            String expected = "" + n;
            assertEquals(expected, new String(journal.read(read, Journal.ReadType.ASYNC), "UTF-8").substring(0, expected.length()));
        }
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024 * 1024 * 16);
        journal.setMaxWriteBatchSize(1024 * 1024 * 2);
        return true;
    }
}