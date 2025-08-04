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

package com.alibaba.polardbx.proxy.handler;

import com.alibaba.polardbx.proxy.context.BackendContext;
import com.alibaba.polardbx.proxy.protocol.handler.result.FieldListResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.OkErrResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.QueryResultHandler;
import com.alibaba.polardbx.proxy.protocol.handler.result.StringResultHandler;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

@Ignore("manual test only")
public class HandlerLeakTest {
    @Test
    public void queryResultLeakTest() throws Exception {
        {
            final AtomicReference<BackendContext> ref = new AtomicReference<>();
            QueryResultHandler handler = new QueryResultHandler(ref, null, null, null); // leak it
            handler.setKillWhenLeak(false);
            handler = null;
        }

        System.gc();
        Thread.sleep(100);
    }

    @Test
    public void fieldListResultLeakTest() throws Exception {
        {
            final AtomicReference<BackendContext> ref = new AtomicReference<>();
            FieldListResultHandler handler = new FieldListResultHandler(ref, null, null, null); // leak it
            handler.setKillWhenLeak(false);
            handler = null;
        }

        System.gc();
        Thread.sleep(100);
    }

    @Test
    public void okErrResultLeakTest() throws Exception {
        {
            final AtomicReference<BackendContext> ref = new AtomicReference<>();
            OkErrResultHandler handler = new OkErrResultHandler(ref, null, null, null); // leak it
            handler.setKillWhenLeak(false);
            handler = null;
        }

        System.gc();
        Thread.sleep(100);
    }

    @Test
    public void stringResultLeakTest() throws Exception {
        {
            final AtomicReference<BackendContext> ref = new AtomicReference<>();
            StringResultHandler handler = new StringResultHandler(ref, null, null, null); // leak it
            handler.setKillWhenLeak(false);
            handler = null;
        }

        System.gc();
        Thread.sleep(100);
    }
}
