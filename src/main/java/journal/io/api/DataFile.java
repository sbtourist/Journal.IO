/**
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package journal.io.api;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import journal.io.util.IOHelper;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
class DataFile implements Comparable<DataFile> {

    private volatile File file;
    private final Integer dataFileId;
    private volatile Integer dataFileGeneration;
    private final AtomicLong length;
    private volatile DataFile next;

    DataFile(File file, int id) {
        this.file = file;
        this.dataFileId = id;
        this.dataFileGeneration = 0;
        this.length = new AtomicLong((file.exists() ? file.length() : 0));
    }

    File getFile() {
        return file;
    }

    Integer getDataFileId() {
        return dataFileId;
    }

    Integer getDataFileGeneration() {
        return dataFileGeneration;
    }

    public void setDataFileGeneration(Integer dataFileGeneration) {
        this.dataFileGeneration = dataFileGeneration;
    }
    
    void incrementGeneration() {
        this.dataFileGeneration++;
    }

    DataFile getNext() {
        return next;
    }

    void setNext(DataFile next) {
        this.next = next;
    }

    long getLength() {
        return this.length.get();
    }

    void setLength(long length) {
        this.length.set(length);
    }

    void incrementLength(int size) {
        this.length.addAndGet(size);
    }

    RandomAccessFile openRandomAccessFile() throws IOException {
        return new RandomAccessFile(file, "rw");
    }

    boolean delete() throws IOException {
        return IOHelper.deleteFile(file);
    }

    void move(File targetDirectory) throws IOException {
        IOHelper.moveFile(file, targetDirectory);
    }
    
    void rename(File destination) throws IOException {
        IOHelper.renameFile(file, destination);
        this.file = destination;
    }
    
    void writeHeader() throws IOException {
        RandomAccessFile raf = openRandomAccessFile();
        try {
            raf.write(Journal.MAGIC_STRING);
            raf.writeInt(Journal.STORAGE_VERSION);
            length.set(Journal.FILE_HEADER_SIZE);
        } finally {
            raf.close();
        }
    }
    
    void verifyHeader() throws IOException {
        RandomAccessFile raf = openRandomAccessFile();
        try {
            byte[] magic = new byte[Journal.MAGIC_SIZE];
            if (raf.read(magic) == Journal.MAGIC_SIZE && Arrays.equals(magic, Journal.MAGIC_STRING)) {
                int version = raf.readInt();
                if (version != Journal.STORAGE_VERSION) {
                    throw new IllegalStateException("Incompatible storage version, found: " + version + ", required: " + Journal.STORAGE_VERSION);
                }
            } else {
                throw new IOException("Incompatible magic string!");
            }
        } finally {
            raf.close();
        }
    }

    @Override
    public int compareTo(DataFile df) {
        return dataFileId - df.dataFileId;
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof DataFile) {
            result = compareTo((DataFile) o) == 0;
        }
        return result;
    }

    @Override
    public int hashCode() {
        return dataFileId;
    }

    @Override
    public String toString() {
        return file.getName() + ", number = " + dataFileId + ", generation = " + dataFileGeneration + ", length = " + length;
    }
}
