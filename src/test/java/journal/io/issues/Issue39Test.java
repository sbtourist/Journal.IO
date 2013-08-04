package journal.io.issues;

import org.junit.Test;

import java.util.Random;
import journal.io.api.AbstractJournalTest;
import journal.io.api.Journal;
import journal.io.api.Location;

/**
 * Test for verifying https://github.com/sbtourist/Journal.IO/issues/39 Please
 * note the 1MB journal db size and the 10K batch size.
 *
 * @author sebastian stoll
 */
public class Issue39Test extends AbstractJournalTest {

    @Test
    public void testCompactionAndRedo() {

        byte[] data = getDataFixture(1000);

        System.out.println(journal.getDirectory().getAbsolutePath());

        try {
            //write some data
            writeData(journal, data, 1000);

            //redo and delete all locations
            Iterable<Location> locations = journal.redo();
            for (Location location : locations) {
                journal.delete(location);
            }

            //write more data
            writeData(journal, data, 900);

            //compact the journal
            journal.compact();
            journal.close();
            journal.open();

            //redo and read the journal -> will fail here
            locations = journal.redo();
            for (Location location : locations) {
                journal.read(location, Journal.ReadType.SYNC);
            }
        } catch (Exception e) {
            System.out.println(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean configure(Journal journal) {
        //1MB file + 10K batch sizes
        journal.setMaxFileLength(1024 * 1024);
        journal.setMaxWriteBatchSize(1024 * 10);
        return true;
    }

    private void writeData(Journal journal, byte[] data, int iterations) throws Exception {
        for (int i = 0; i < iterations; i++) {
            journal.write(data, Journal.WriteType.SYNC);
        }
    }

    private byte[] getDataFixture(int fixtureLength) {
        StringBuffer buffer = new StringBuffer();
        char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        Random rnd = new Random();
        for (int i = 0; i < fixtureLength; i++) {
            buffer.append(chars[rnd.nextInt(26)]);
        }
        return buffer.toString().getBytes();
    }
}
