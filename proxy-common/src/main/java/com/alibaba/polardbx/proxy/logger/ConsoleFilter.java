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

package com.alibaba.polardbx.proxy.logger;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class ConsoleFilter extends Filter<ILoggingEvent> {
    private static final boolean isIDE;

    static {
        // always with inst id when online
        final String instId = System.getProperty("polardbx_instance_id");
        isIDE = null == instId || instId.isEmpty();
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (isIDE) {
            // only allow when start with IDE
            return FilterReply.ACCEPT;
        } else {
            return FilterReply.DENY;
        }
    }
}
