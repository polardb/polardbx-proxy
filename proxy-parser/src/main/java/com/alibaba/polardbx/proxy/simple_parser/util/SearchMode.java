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

package com.alibaba.polardbx.proxy.simple_parser.util;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Search mode flags enumeration. Primarily used by {@link StringInspector}.
 */
public enum SearchMode {

    /**
     * Allow backslash escapes.
     */
    ALLOW_BACKSLASH_ESCAPE,
    /**
     * Skip between markers (quoted text, quoted identifiers, text between parentheses).
     */
    SKIP_BETWEEN_MARKERS,
    /**
     * Skip between block comments ("/* text... *\/") but not between hint blocks.
     */
    SKIP_BLOCK_COMMENTS,
    /**
     * Skip line comments ("-- text...", "# text...").
     */
    SKIP_LINE_COMMENTS,
    /**
     * Skip MySQL specific markers ("/*![12345]" and "*\/") but not their contents.
     */
    SKIP_MYSQL_MARKERS,
    /**
     * Skip hint blocks ("/*+ text... *\/").
     */
    SKIP_HINT_BLOCKS,
    /**
     * Skip white space.
     */
    SKIP_WHITE_SPACE,
    /**
     * Dummy search mode. Does nothing.
     */
    VOID;

    /*
     * Convenience EnumSets for several SearchMode combinations
     */

    /**
     * Full search mode: allow backslash escape, skip between markers, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip
     * white space.
     * This is technically equivalent to __BSE_MRK_COM_MYM_HNT_WS.
     */
    public static final Set<SearchMode> __FULL = Collections.unmodifiableSet(EnumSet.allOf(SearchMode.class));

    /**
     * Search mode: allow backslash escape, skip between markers, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip
     * white space.
     */
    public static final Set<SearchMode> __BSE_MRK_COM_MYM_HNT_WS =
        Collections.unmodifiableSet(EnumSet.of(ALLOW_BACKSLASH_ESCAPE, SKIP_BETWEEN_MARKERS,
            SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS, SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: skip between markers, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip white space.
     */
    public static final Set<SearchMode> __MRK_COM_MYM_HNT_WS = Collections
        .unmodifiableSet(EnumSet.of(SKIP_BETWEEN_MARKERS, SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS,
            SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: allow backslash escape, skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip white space.
     */
    public static final Set<SearchMode> __BSE_COM_MYM_HNT_WS = Collections.unmodifiableSet(
        EnumSet.of(ALLOW_BACKSLASH_ESCAPE, SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS,
            SKIP_HINT_BLOCKS, SKIP_WHITE_SPACE));

    /**
     * Search mode: skip block comments, skip line comments, skip MySQL markers, skip hint blocks and skip white space.
     */
    public static final Set<SearchMode> __COM_MYM_HNT_WS = Collections
        .unmodifiableSet(EnumSet.of(SKIP_BLOCK_COMMENTS, SKIP_LINE_COMMENTS, SKIP_MYSQL_MARKERS, SKIP_HINT_BLOCKS,
            SKIP_WHITE_SPACE));

    /**
     * Search mode: allow backslash escape, skip between markers and skip white space.
     */
    public static final Set<SearchMode> __BSE_MRK_WS =
        Collections.unmodifiableSet(EnumSet.of(ALLOW_BACKSLASH_ESCAPE, SKIP_BETWEEN_MARKERS, SKIP_WHITE_SPACE));

    /**
     * Search mode: skip between markers and skip white space.
     */
    public static final Set<SearchMode> __MRK_WS =
        Collections.unmodifiableSet(EnumSet.of(SKIP_BETWEEN_MARKERS, SKIP_WHITE_SPACE));

    /**
     * Empty search mode.
     * There must be at least one element so that the Set may be later duplicated if needed.
     */
    public static final Set<SearchMode> __NONE = Collections.unmodifiableSet(EnumSet.of(VOID));

}
