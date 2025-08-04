/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.proxy.callback;

import com.alibaba.polardbx.proxy.protocol.common.ResultState;
import com.alibaba.polardbx.proxy.protocol.handler.result.ResultHandler;

public interface ResultCallback {
    /**
     * Invoke when state change, hold the handler lock.
     * Used to record any state change or time.
     *
     * @param handler the result handler
     * @param before before state
     * @param state after state
     */
    void onStateChangeWithinLock(ResultHandler handler, ResultState before, ResultState state);

    /**
     * Invoke when all operation done(include forward) and not hold the handler lock.
     *
     * @param handler the result handler
     * @param lastValidState the last valid state
     * @param state final state(most likely to be done, or exception in state changed to abort)
     */
    default void onDone(ResultHandler handler, ResultState lastValidState, ResultState state) {
    }
}
