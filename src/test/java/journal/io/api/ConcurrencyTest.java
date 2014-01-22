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
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Sergio Bossa
 */
public class ConcurrencyTest extends AbstractJournalTest {

    @Test
    public void testConcurrentWriteAndRead() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        int iterations = 1000;
        //
        for (int i = 0; i < iterations; i++) {
            final int index = i;
            executor.submit(new Runnable() {
                public void run() {
                    try {
                        Journal.WriteType sync = index % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
                        String write = new String("DATA" + index);
                        Location location = journal.write(write.getBytes("UTF-8"), sync);
                        String read = new String(journal.read(location, Journal.ReadType.ASYNC), "UTF-8");
                        if (read.equals("DATA" + index)) {
                            counter.incrementAndGet();
                        } else {
                            System.out.println(write);
                            System.out.println(read);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(iterations, counter.get());
    }

    @Test
    public void testConcurrentCompactionDuringWriteAndRead() throws Exception {
        Random rnd = new Random();
        for (int i = 0; i < 50; i++) {
            int iterations = rnd.nextInt(10000);
            int deletions = rnd.nextInt(iterations);
            System.out.println("testConcurrentCompactionDuringWriteAndRead with number of iterations: " + iterations + " and deletions: " + deletions);
            doTestConcurrentCompactionDuringWriteAndRead(iterations, deletions);
            tearDown();
            setUp();
        }
    }

    @Test
    public void testConcurrentCompactionDuringRedoDeletes() throws Exception {
        Random rnd = new Random();
        for (int i = 0; i < 50; i++) {
            int iterations = rnd.nextInt(10000);
            int deletions = rnd.nextInt(iterations);
            System.out.println("testConcurrentCompactionDuringRedoDeletes with number of iterations: " + iterations + " and deletions: " + deletions);
            doTestConcurrentCompactionDuringRedoDeletes(iterations, deletions);
            tearDown();
            setUp();
        }
    }

    @Override
    protected boolean configure(Journal journal) {
        journal.setMaxFileLength(1024);
        journal.setMaxWriteBatchSize(1024);
        return true;
    }

    private void doTestConcurrentCompactionDuringWriteAndRead(final int iterations, final int deletions) throws InterruptedException, IOException {
        final AtomicInteger iterationsCounter = new AtomicInteger(0);
        final AtomicInteger deletionsCounter = new AtomicInteger(0);
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        //
        for (int i = 0; i < iterations; i++) {
            final int index = i;
            executor.submit(new Runnable() {
                public void run() {
                    try {
                        Random rnd = new Random();
                        Journal.WriteType sync = index % 2 == 0 ? Journal.WriteType.SYNC : Journal.WriteType.ASYNC;
                        String write = new String("DATA" + index);
                        Location location = journal.write(write.getBytes("UTF-8"), sync);
                        String read = new String(journal.read(location, Journal.ReadType.ASYNC), "UTF-8");
                        if (read.equals("DATA" + index)) {
                            int thisDeletion = -1;
                            if ((iterations - iterationsCounter.incrementAndGet() < deletions || rnd.nextInt() % 2 == 0)
                                    && (thisDeletion = deletionsCounter.incrementAndGet()) <= deletions) {
                                journal.delete(location);
                                if (thisDeletion == deletions) {
                                    journal.compact();
                                }
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
        //
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(iterations, iterationsCounter.get());
        int locations = 0;
        for (Location current : journal.redo()) {
            locations++;
        }
        assertEquals(iterations - deletions, locations);
    }

    private void doTestConcurrentCompactionDuringRedoDeletes(final int iterations, final int deletions) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CountDownLatch deletionsLatch = new CountDownLatch(1);
        //
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        //
        executor.submit(new Callable() {
            public Object call() {
                Location ignored = null;
                try {
                    int i = 0;
                    for (Location location : journal.redo()) {
                        i++;
                        if (i <= deletions) {
                            journal.delete(location);
                            if (i == deletions) {
                                deletionsLatch.countDown();
                            }
                        } else {
                            ignored = location;
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                return ignored;
            }
        });
        //
        executor.submit(new Runnable() {
            public void run() {
                try {
                    deletionsLatch.await();
                    journal.compact();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        //
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        int locations = 0;
        for (Location current : journal.redo()) {
            locations++;
        }
        assertEquals(iterations - deletions, locations);
    }
}
