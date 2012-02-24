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

/**
 * Callback interface, providing methods notified after write syncing either succeeds or fails.
 *
 * @author Sergio Bossa
 */
public interface WriteCallback {

    /**
     * Method called after successful write syncing.
     * 
     * @param syncedLocation 
     */
    void onSync(Location syncedLocation);
    
    /**
     * Method called after failed write syncing.
     * 
     * @param location
     * @param error 
     */
    void onError(Location location, Throwable error);
}
