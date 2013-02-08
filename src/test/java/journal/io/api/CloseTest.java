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

import org.junit.Test;

/**
 * @author Sergio Bossa
 */
public class CloseTest extends AbstractJournalTest {

    @Test(expected = ClosedJournalException.class)
    public void testWritingFailsAfterClose() throws Exception {
        journal.close();
        journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
    }

    @Test(expected = ClosedJournalException.class)
    public void testReadingFailsAfterClose() throws Exception {
        Location location = journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
        journal.close();
        journal.read(location, Journal.ReadType.SYNC);
    }

    @Test(expected = ClosedJournalException.class)
    public void testDeletingFailsAfterClose() throws Exception {
        Location location = journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
        journal.close();
        journal.delete(location);
    }

    @Test(expected = ClosedJournalException.class)
    public void testRedoingFailsAfterClose() throws Exception {
        Location location = journal.write("data".getBytes("UTF-8"), Journal.WriteType.SYNC);
        journal.close();
        journal.redo();
    }

    @Test(expected = ClosedJournalException.class)
    public void testCompactingFailsAfterClose() throws Exception {
        journal.close();
        journal.compact();
    }
}
