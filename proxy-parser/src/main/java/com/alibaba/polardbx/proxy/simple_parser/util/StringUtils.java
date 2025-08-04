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

import java.util.Set;

public class StringUtils {
    protected static boolean isCharEqualIgnoreCase(char charToCompare, char compareToCharUC, char compareToCharLC) {
        return Character.toLowerCase(charToCompare) == compareToCharLC
            || Character.toUpperCase(charToCompare) == compareToCharUC;
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string 'searchFor', disregarding case and starting at 'startAt'. Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn the string to search in
     * @param startAt the position to start at
     * @param searchFor the string to search for
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean regionMatchesIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * Finds the position of the next alphanumeric character within a string, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition the position to start the search from
     * @param searchIn the string to search in
     * @param openingMarkers characters that delimit the beginning of a text block to skip
     * @param closingMarkers characters that delimit the end of a text block to skip
     * @param overridingMarkers the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     * <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     * otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param searchMode a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     * behavior of the search
     * @return the position where the next non-whitespace character is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfNextAlphanumericChar(int startingPosition, String searchIn, String openingMarkers,
                                                  String closingMarkers, String overridingMarkers,
                                                  Set<SearchMode> searchMode) {
        StringInspector strInspector =
            new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, overridingMarkers,
                searchMode);
        return strInspector.indexOfNextAlphanumericChar();
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn the string to search in
     * @param searchFor the string to search for
     * @param beginPos where to start searching
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        for (; beginPos < searchIn.length(); beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return regionMatchesIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Finds the position of a substring within a string ignoring case.
     *
     * @param searchIn the string to search in
     * @param searchFor the array of strings to search for
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(String searchIn, String searchFor) {
        return indexOfIgnoreCase(0, searchIn, searchFor);
    }

    private static boolean isCharAtPosNotEqualIgnoreCase(String searchIn, int pos, char firstCharOfSearchForUc,
                                                         char firstCharOfSearchForLc) {
        return Character.toLowerCase(searchIn.charAt(pos)) != firstCharOfSearchForLc
            && Character.toUpperCase(searchIn.charAt(pos)) != firstCharOfSearchForUc;
    }

    /**
     * Finds the position of a substring within a string ignoring case.
     *
     * @param startingPosition the position to start the search from
     * @param searchIn the string to search in
     * @param searchFor the array of strings to search for
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor) {
        if (searchIn == null || searchFor == null) {
            return -1;
        }

        int searchInLength = searchIn.length();
        int searchForLength = searchFor.length();
        int stopSearchingAt = searchInLength - searchForLength;

        if (startingPosition > stopSearchingAt || searchForLength == 0) {
            return -1;
        }

        // Some locales don't follow upper-case rule, so need to check both
        char firstCharOfSearchForUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfSearchForLc = Character.toLowerCase(searchFor.charAt(0));

        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            if (isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc, firstCharOfSearchForLc)) {
                // find the first occurrence of the first character of searchFor in searchIn
                while (++i <= stopSearchingAt && isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc,
                    firstCharOfSearchForLc)) {
                }
            }

            if (i <= stopSearchingAt && regionMatchesIgnoreCase(searchIn, i, searchFor)) {
                return i;
            }
        }

        return -1;
    }
}
