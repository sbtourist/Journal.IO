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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Represent a location inside the journal.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public final class Location implements Comparable<Location> {

    static final byte ANY_RECORD_TYPE = 0;
    static final byte USER_RECORD_TYPE = 1;
    static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    static final byte DELETED_RECORD_TYPE = 3;
    //
    static final int NOT_SET = -1;
    //
    // The data file id this location refers to:
    private volatile int dataFileId = NOT_SET;
    // The data file generation this location refers to 
    // (used to check if a file has been compacted since the location has been read):
    private volatile int dataFileGeneration = NOT_SET;
    // Position in the file of this location:
    private volatile long thisFilePosition = NOT_SET;
    // Position in the file of next location:
    private volatile long nextFilePosition = NOT_SET;
    // Intra-file poiner, total location size and type:
    private volatile int pointer = NOT_SET;
    private volatile int size = NOT_SET;
    private volatile byte type = ANY_RECORD_TYPE;
    //
    private volatile WriteCallback writeCallback = NoWriteCallback.INSTANCE;
    private volatile byte[] data;
    private volatile CountDownLatch latch;

    public Location() {
    }

    public Location(Location source) {
        this.dataFileId = source.dataFileId;
        this.dataFileGeneration = source.dataFileGeneration;
        this.thisFilePosition = source.thisFilePosition;
        this.nextFilePosition = source.nextFilePosition;
        this.pointer = source.pointer;
        this.size = source.size;
        this.type = source.type;
    }

    public Location(int dataFileId) {
        this.dataFileId = dataFileId;
    }

    public Location(int dataFileId, int pointer) {
        this.dataFileId = dataFileId;
        this.pointer = pointer;
    }

    public boolean isBatchControlRecord() {
        return dataFileId != NOT_SET && type == Location.BATCH_CONTROL_RECORD_TYPE;
    }

    public boolean isDeletedRecord() {
        return dataFileId != NOT_SET && type == Location.DELETED_RECORD_TYPE;
    }

    public boolean isUserRecord() {
        return dataFileId != NOT_SET && type == Location.USER_RECORD_TYPE;
    }

    public int getSize() {
        return size;
    }

    public int getPointer() {
        return pointer;
    }

    public int getDataFileId() {
        return dataFileId;
    }

    public byte[] getData() {
        return data;
    }

    void setSize(int size) {
        this.size = size;
    }

    void setPointer(int pointer) {
        this.pointer = pointer;
    }

    void setDataFileId(int file) {
        this.dataFileId = file;
    }

    byte getType() {
        return type;
    }

    void setType(byte type) {
        this.type = type;
    }

    CountDownLatch getLatch() {
        return latch;
    }

    void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    void setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
    }

    WriteCallback getWriteCallback() {
        return writeCallback;
    }

    void setData(byte[] data) {
        this.data = data;
    }

    long getThisFilePosition() {
        return thisFilePosition;
    }

    void setThisFilePosition(long thisFilePosition) {
        this.thisFilePosition = thisFilePosition;
    }

    long getNextFilePosition() {
        return nextFilePosition;
    }

    void setNextFilePosition(long nextFilePosition) {
        this.nextFilePosition = nextFilePosition;
    }

    void setDataFileGeneration(int dataFileGeneration) {
        this.dataFileGeneration = dataFileGeneration;
    }

    int getDataFileGeneration() {
        return dataFileGeneration;
    }

    public String toString() {
        return dataFileId + ":" + pointer + ":" + type;
    }

    public void writeExternal(DataOutput dos) throws IOException {
        dos.writeInt(dataFileId);
        dos.writeInt(dataFileGeneration);
        dos.writeLong(thisFilePosition);
        dos.writeLong(nextFilePosition);
        dos.writeInt(pointer);
        dos.writeInt(size);
        dos.writeByte(type);
    }

    public void readExternal(DataInput dis) throws IOException {
        dataFileId = dis.readInt();
        dataFileGeneration = dis.readInt();
        thisFilePosition = dis.readLong();
        nextFilePosition = dis.readLong();
        pointer = dis.readInt();
        size = dis.readInt();
        type = dis.readByte();
    }

    public int compareTo(Location o) {
        if (dataFileId == o.dataFileId) {
            int rc = pointer - o.pointer;
            return rc;
        }
        return dataFileId - o.dataFileId;
    }

    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof Location) {
            result = compareTo((Location) o) == 0;
        }
        return result;
    }

    public int hashCode() {
        return dataFileId ^ pointer;
    }

    static class NoWriteCallback implements WriteCallback {

        public static final WriteCallback INSTANCE = new NoWriteCallback();

        @Override
        public void onSync(Location syncedLocation) {
        }

        @Override
        public void onError(Location location, Throwable error) {
        }
    }
}
