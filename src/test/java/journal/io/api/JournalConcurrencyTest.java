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

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import journal.io.AbstractJournalTest;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class JournalConcurrencyTest extends AbstractJournalTest {

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
        final AtomicInteger iterationsCounter = new AtomicInteger(0);
        final AtomicInteger deletionsCounter = new AtomicInteger(0);
        final CountDownLatch deletionsLatch = new CountDownLatch(1);
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final int iterations = 1000;
        final int deletions = 100;
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
                            if (index < deletions) {
                                journal.delete(location);
                                deletionsCounter.incrementAndGet();
                            }
                            if (deletionsCounter.get() == deletions) {
                                deletionsLatch.countDown();
                            }
                            iterationsCounter.incrementAndGet();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
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
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertEquals(iterations, iterationsCounter.get());
        int locations = 0;
        for (Location current : journal.redo()) {
            locations++;
        }
        assertEquals(iterations - deletions, locations);
        assertTrue(journal.getDataFiles().firstEntry().getKey() > 1);
    }

    @Test
    public void testConcurrentCompactionDuringRedoDeletes() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final AtomicInteger iterationsCounter = new AtomicInteger(0);
        final CountDownLatch deletionsLatch = new CountDownLatch(1);
        final int iterations = 1000;
        final int deletions = 100;
        //
        for (int i = 0; i < iterations; i++) {
            journal.write(new String("DATA" + i).getBytes("UTF-8"), Journal.WriteType.SYNC);
        }
        //
        executor.submit(new Runnable() {
            public void run() {
                try {
                    int i = 0;
                    for (Location location : journal.redo()) {
                        journal.delete(location);
                        if (++i == deletions) {
                            deletionsLatch.countDown();
                            break;
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
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
        executor.submit(new Runnable() {
            public void run() {
                try {
                    deletionsLatch.await();
                    for (Location current : journal.redo()) {
                        iterationsCounter.incrementAndGet();
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        //
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        assertTrue(journal.getDataFiles().firstEntry().getKey() > 1);
        assertEquals(iterations - deletions, iterationsCounter.get());
    }

    @Override
    protected void configure(Journal journal) {
        journal.setMaxFileLength(1024);
        journal.setMaxWriteBatchSize(1024);
    }
}
