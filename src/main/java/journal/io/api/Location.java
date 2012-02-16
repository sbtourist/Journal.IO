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

    static final byte NO_RECORD_TYPE = 0;
    static final byte USER_RECORD_TYPE = 1;
    static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    static final byte DELETED_RECORD_TYPE = 3;
    //
    static final int NOT_SET = -1;
    //
    private volatile int dataFileId = NOT_SET;
    private volatile int pointer = NOT_SET;
    private volatile int size = NOT_SET;
    private volatile byte type = NO_RECORD_TYPE;
    private volatile WriteCallback writeCallback = NoWriteCallback.INSTANCE;
    private volatile byte[] data;
    private CountDownLatch latch;

    public Location() {
    }

    public Location(Location item) {
        this.dataFileId = item.dataFileId;
        this.pointer = item.pointer;
        this.size = item.size;
        this.type = item.type;
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

    public void setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
    }

    public WriteCallback getWriteCallback() {
        return writeCallback;
    }

    void setData(byte[] data) {
        this.data = data;
    }

    public String toString() {
        return dataFileId + ":" + pointer;
    }

    public void writeExternal(DataOutput dos) throws IOException {
        dos.writeInt(dataFileId);
        dos.writeInt(pointer);
        dos.writeInt(size);
        dos.writeByte(type);
    }

    public void readExternal(DataInput dis) throws IOException {
        dataFileId = dis.readInt();
        pointer = dis.readInt();
        size = dis.readInt();
        type = dis.readByte();
    }

    public int compareTo(Location o) {
        Location l = o;
        if (dataFileId == l.dataFileId) {
            int rc = pointer - l.pointer;
            return rc;
        }
        return dataFileId - l.dataFileId;
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

    public static class NoWriteCallback implements WriteCallback {

        public static final WriteCallback INSTANCE = new NoWriteCallback();

        @Override
        public void onSync(Location syncedLocation) {
        }

        @Override
        public void onError(Location location, Throwable error) {
        }
    }
}
