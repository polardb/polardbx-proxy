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

package com.alibaba.polardbx.proxy.utils;

import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LeakChecker implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeakChecker.class);
    private static final Cleaner CLEANER = Cleaner.create();

    private static final class CleanAction implements LeakCallback {
        private final AtomicBoolean closed;
        private final LeakCallback callback;
        private String tag;
        @Setter
        private boolean killWhenLeak = true;

        public CleanAction(@NotNull final AtomicBoolean closed, final LeakCallback callback) {
            this.closed = closed;
            this.callback = callback;
        }

        @Override
        public void setTag(String tag) {
            this.tag = tag;
        }

        @Override
        public void run() {
            // full fence read
            if (closed.get()) {
                return; // ignore normal closed
            }
            if (null == callback) {
                LOGGER.error("Resource (tag: {}) leak detected{}.", null == tag ? "<null>" : tag,
                    killWhenLeak ? ", abort process" : "");
                if (killWhenLeak) {
                    Kill.kill9();
                }
            } else {
                callback.run();
            }
        }
    }

    protected final AtomicBoolean leakCheckClosed = new AtomicBoolean(false);
    private final CleanAction cleanAction;
    private final LeakCallback leakCallback;
    private final Cleaner.Cleanable cleanable;

    public LeakChecker() {
        this(null);
    }

    public LeakChecker(final LeakCallback staticLeakCallback) {
        this.cleanAction = new CleanAction(leakCheckClosed, staticLeakCallback);
        this.leakCallback = null == staticLeakCallback ? this.cleanAction : staticLeakCallback;
        this.cleanable = CLEANER.register(this, this.cleanAction);
    }

    public void setTag(String tag) {
        leakCallback.setTag(tag);
    }

    public void setKillWhenLeak(boolean killWhenLeak) {
        cleanAction.setKillWhenLeak(killWhenLeak);
    }

    @Override
    public void close() {
        if (!leakCheckClosed.getPlain()) {
            leakCheckClosed.set(true); // full fence set
        }
        cleanable.clean();
    }
}
