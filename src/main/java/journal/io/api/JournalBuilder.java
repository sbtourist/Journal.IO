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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A builder style API to create ready to use {@link Journal}.
 */
public class JournalBuilder {

    private final File directory;
    private File directoryArchive;
    private Boolean checksum;
    private Long disposeInterval;
    private ScheduledExecutorService disposer;
    private String filePrefix;
    private String fileSuffix;
    private Integer maxFileLength;
    private Integer maxWriteBatchSize;
    private Boolean physicalSync;
    private RecoveryErrorHandler recoveryErrorHandler;
    private ReplicationTarget replicationTarget;
    private Executor writer;

    private JournalBuilder(final File directory) {
        //Checks directory exists and is a directory
        //Also ensures current user has write access
        if (!directory.exists()) {
            throw new IllegalArgumentException("<"+directory+"> does not exist");
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("<"+directory+"> is not a directory");
        }
        if (!directory.canWrite()) {
            throw new IllegalArgumentException("Cannot write to main directory <"+directory+">");
        }

        this.directory = directory;
    }

    public JournalBuilder setArchived(final File to) {
        this.directoryArchive = to;
        return this;
    }

    public JournalBuilder setChecksum(final Boolean checksum) {
        this.checksum = checksum;
        return this;
    }

    public JournalBuilder setDisposeInterval(final Long disposeInterval) {
        this.disposeInterval = disposeInterval;
        return this;
    }

    public JournalBuilder setDisposer(final ScheduledExecutorService disposer) {
      this.disposer = disposer;
      return this;
    }

    public JournalBuilder setFilePrefix(final String filePrefix) {
        this.filePrefix = filePrefix;
        return this;
    }

    public JournalBuilder setFileSuffix(final String fileSuffix) {
        this.fileSuffix = fileSuffix;
        return this;
    }

    public JournalBuilder setMaxFileLength(final Integer maxFileLength) {
        this.maxFileLength = maxFileLength;
        return this;
    }

    public JournalBuilder setMaxWriteBatchSize(final Integer maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
        return this;
    }

    public JournalBuilder setPhysicalSync(final Boolean physicalSync) {
        this.physicalSync = physicalSync;
        return this;
    }

    public JournalBuilder setRecoveryErrorHandler(final RecoveryErrorHandler recoveryErrorHandler) {
        this.recoveryErrorHandler = recoveryErrorHandler;
        return this;
    }

    public JournalBuilder setReplicationTarget(final ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
        return this;
    }

    public JournalBuilder setWriter(final Executor writer) {
        this.writer = writer;
        return this;
    }

    /**
     * @return a configured and opened {@link Journal}
     * @throws IOException 
     */
    public Journal open() throws IOException {
        final Journal journal = new Journal();
        journal.setDirectory(this.directory);
        if (this.directoryArchive != null) {
            journal.setArchiveFiles(true);
            journal.setDirectoryArchive(this.directoryArchive);
        }
        if (this.checksum != null) {
            journal.setChecksum(this.checksum);
        }
        if (this.disposeInterval != null) {
            journal.setDisposeInterval(this.disposeInterval);
        }
        journal.setDisposer(this.disposer);
        journal.setFilePrefix(this.filePrefix);
        journal.setFileSuffix(this.fileSuffix);
        if (this.maxFileLength  != null) {
            journal.setMaxFileLength(this.maxFileLength);
        }
        if (this.maxWriteBatchSize != null) {
            journal.setMaxWriteBatchSize(this.maxWriteBatchSize);
        }
        if (this.physicalSync != null) {
            journal.setPhysicalSync(this.physicalSync);
        }
        journal.setRecoveryErrorHandler(this.recoveryErrorHandler);
        journal.setReplicationTarget(this.replicationTarget);
        journal.setWriter(this.writer);
        journal.open();
        return journal;
    }

  /**
   * @param directory
   * @return a {@link JournalBuilder} using {@code directory} as base directory
   */
  public static JournalBuilder of(final File directory) {
      return new JournalBuilder(directory);
  }

}