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

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class LifeCycleTest {

    @Test(expected=IOException.class)
    public void testInvalidDirectoryIsRejected() throws IOException {
      final File file = new File("non-existing");
      final Journal localournal = new Journal();
      localournal.setDirectory(file);
      try {
          localournal.open();
      } finally {
          localournal.close();
      }
    }

}