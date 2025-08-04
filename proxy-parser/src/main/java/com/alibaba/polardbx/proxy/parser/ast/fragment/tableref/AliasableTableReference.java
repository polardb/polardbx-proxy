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
/**
 * (created at 2011-7-27)
 */
package com.alibaba.polardbx.proxy.parser.ast.fragment.tableref;

import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralString;

/**
 * @author QIU Shuo
 */
public abstract class AliasableTableReference implements TableReference {

    protected String alias;
    protected String aliasUnEscape;
    protected String aliasUpUnEscape;

    public AliasableTableReference(String alias) {
        this.alias = alias;

        this.aliasUnEscape = aliasUnescapeUppercase(alias, false);
        this.aliasUpUnEscape = aliasUnescapeUppercase(alias, false);
    }

    /**
     * @return upper-case, empty is possible
     */
    public String aliasUnescapeUppercase(String alias, boolean toUppercase) {
        if (alias == null || alias.length() <= 0) {
            return alias;
        }
        switch (alias.charAt(0)) {
        case '`':
            return Identifier.unescapeName(alias, toUppercase);
        case '\'':
            return LiteralString.getUnescapedString(alias.substring(1, alias.length() - 1), toUppercase);
        case '_':
            int ind = -1;
            for (int i = 1; i < alias.length(); ++i) {
                if (alias.charAt(i) == '\'') {
                    ind = i;
                    break;
                }
            }
            if (ind >= 0) {
                LiteralString st = new LiteralString(alias.substring(0, ind), alias.substring(ind + 1,
                    alias.length() - 1), false);
                return aliasUpUnEscape = st.getUnescapedString(toUppercase);
            }
        default:
            if (toUppercase) {
                return alias.toUpperCase();
            } else {
                return alias;
            }
        }
    }

    public String getAlias() {
        return alias;
    }

    public AliasableTableReference setAlias(String alias) {
        this.alias = alias;

        this.aliasUnEscape = aliasUnescapeUppercase(alias, false);
        this.aliasUpUnEscape = aliasUnescapeUppercase(alias, false);
        return this;
    }

    public String getAliasUnEscape() {
        return aliasUnEscape;
    }

    public String getAliasUpUnEscape() {
        return aliasUpUnEscape;
    }

}
