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

package com.alibaba.polardbx.proxy.simple_parser;

import com.alibaba.polardbx.proxy.simple_parser.util.SearchMode;
import com.alibaba.polardbx.proxy.simple_parser.util.StringInspector;
import com.alibaba.polardbx.proxy.simple_parser.util.StringUtils;

public class SimpleParser {
    private static final String OPENING_MARKERS = "`'\"";
    private static final String CLOSING_MARKERS = "`'\"";
    private static final String OVERRIDING_MARKERS = "";

    private final String originalSql;
    private final String sqlMode;

    private final boolean isNoBackslashEscapesSet;
    private final StringInspector inspector;
    private final int statementKeywordPos;

    // lazy generated data
    private String sanitizedSql;

    public SimpleParser(String originalSql, String sqlMode) {
        this.originalSql = originalSql;
        this.sqlMode = sqlMode;
        this.isNoBackslashEscapesSet = sqlMode != null && sqlMode.contains("NO_BACKSLASH_ESCAPES");
        this.inspector = new StringInspector(originalSql, OPENING_MARKERS, CLOSING_MARKERS, OVERRIDING_MARKERS,
            this.isNoBackslashEscapesSet ? SearchMode.__MRK_COM_MYM_HNT_WS : SearchMode.__BSE_MRK_COM_MYM_HNT_WS);
        this.statementKeywordPos = this.inspector.indexOfNextAlphanumericChar();
        this.inspector.mark();
    }

    public boolean isSelect() {
        if (-1 == statementKeywordPos) {
            return false;
        }
        final char c = originalSql.charAt(statementKeywordPos);
        if (c != 'S' && c != 's') {
            return false;
        }
        return StringUtils.regionMatchesIgnoreCase(originalSql, statementKeywordPos, "SELECT");
    }

    public synchronized String getSanitizedSql() {
        if (null == sanitizedSql) {
            sanitizedSql = inspector.stripCommentsAndHints();
        }
        return sanitizedSql;
    }

    public boolean canSlaveRead() {
        // only allow select
        if (!isSelect()) {
            return false;
        }

        // SELECT with PROCEDURE syntax is deprecated as of MySQL 5.7.18, and is removed in MySQL 8.0.

        // So just check lock in share mode or for update, route to leader if lock exists.
        synchronized (inspector) {
            inspector.reset(); // reset to mark
            inspector.setStartPosition(statementKeywordPos + 7); // skip "select_"
            if (inspector.indexOfIgnoreCase("LOCK", "IN", "SHARE", "MODE") != -1) {
                return false;
            }
            inspector.reset(); // reset to mark
            inspector.setStartPosition(statementKeywordPos + 7); // skip "select_"
            return -1 == inspector.indexOfIgnoreCase("FOR", "UPDATE");
        }
    }

    public boolean isSet() {
        if (-1 == statementKeywordPos) {
            return false;
        }
        final char c = originalSql.charAt(statementKeywordPos);
        if (c != 'S' && c != 's') {
            return false;
        }
        return StringUtils.startsWithIgnoreCaseAndWs(originalSql, "SET", statementKeywordPos);
    }
}
