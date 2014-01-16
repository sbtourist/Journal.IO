package journal.io.issues;

import java.io.File;
import java.io.IOException;
import journal.io.api.Journal;
import org.junit.Test;

public class Issue55Test {

    private static final File ISSUE55_DIR = new File(Issue55Test.class.getClassLoader().getResource("issue55").getFile());

    @Test
    public void test() throws IOException {
        Journal journal = new Journal();
        journal.setDirectory(ISSUE55_DIR);
        journal.open();
        journal.close();
    }
}
