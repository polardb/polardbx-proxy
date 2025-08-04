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

package com.alibaba.polardbx.proxy.scheduler;

import com.alibaba.polardbx.proxy.connection.pool.BackendConnectionWrapper;
import com.alibaba.polardbx.proxy.context.FrontendContext;
import com.alibaba.polardbx.proxy.context.transaction.FrontendTransactionContext;

import java.io.IOException;

public class InitBackendTask implements ScheduleTask {
    @Override
    public Boolean forward(Scheduler scheduler) throws Exception {
        // start trx and get connection if needed
        if (null == scheduler.getBackend()) {
            final FrontendContext context = scheduler.getContext();
            final boolean goSlave = scheduler.getSlaveRead() != null && scheduler.getSlaveRead();
            final FrontendTransactionContext transaction = context.referenceTransaction(!goSlave);
            assert transaction != null;
            scheduler.setDereference(true);
            BackendConnectionWrapper backend =
                goSlave ? transaction.getRoConnection(context) : transaction.getRwConnection(context);
            if (null == backend && goSlave) {
                backend = transaction.getRwConnection(context);
            }
            if (null == backend) {
                throw new IOException("no backend connection available");
            }
            scheduler.setBackend(backend);
        }
        return null;
    }
}
