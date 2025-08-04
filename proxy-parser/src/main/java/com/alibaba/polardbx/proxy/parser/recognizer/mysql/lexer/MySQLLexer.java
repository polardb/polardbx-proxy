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

package com.alibaba.polardbx.proxy.parser.recognizer.mysql.lexer;

import com.alibaba.polardbx.proxy.parser.recognizer.mysql.MySQLToken;
import com.alibaba.polardbx.proxy.parser.util.FastCharTypes;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

public final class MySQLLexer {
    public static final int DEFAULT_VERSION = 80032;

    @Getter
    private final byte[] sql;
    @Getter
    private final Charset charset;
    private final boolean noBackslashEscapes;
    private final int version;
    private final int limit;

    @Getter
    private int pos;
    private byte ch;

    @Getter
    @Setter
    private boolean recordComments = false;
    @Getter
    private List<byte[]> comments;

    @Getter
    private MySQLToken lastToken;
    private MySQLToken tokenCache;
    private MySQLToken tokenCache2;
    private MySQLToken token;

    private boolean inCStyleComment = false;
    private boolean inCStyleCommentIgnore;
    @Getter
    private int offsetCache;
    @Getter
    private int sizeCache;

    private int paramIndex = 0;
    @Getter
    private int lastTokenIndex = 0;

    private final static ThreadLocal<byte[]> sbufRef = new ThreadLocal<>();
    private byte[] sbuf;
    private String stringValue = null;

    public MySQLLexer(final String sql) {
        this(sql.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8, false);
    }

    public MySQLLexer(final String sql, final int version) {
        this(sql.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8, false, version, 0,
            sql.getBytes(StandardCharsets.UTF_8).length);
    }

    public MySQLLexer(final byte @NotNull [] sql, @NotNull final Charset charset, final boolean noBackslashEscapes) {
        this(sql, charset, noBackslashEscapes, DEFAULT_VERSION, 0, sql.length);
    }

    public MySQLLexer(final byte @NotNull [] sql, @NotNull final Charset charset, final boolean noBackslashEscapes,
                      final int version, final int pos, final int limit) {
        this.sql = sql;
        this.charset = charset;
        this.noBackslashEscapes = noBackslashEscapes;
        this.version = version;
        this.limit = limit;
        this.pos = pos;
        // load first
        if (pos < limit) {
            this.ch = sql[pos];
        }

        // init rebuild buffer
        if ((this.sbuf = sbufRef.get()) == null) {
            this.sbuf = new byte[1024];
            sbufRef.set(this.sbuf);
        }
    }

    public void reset() {
        this.pos = 0;
        // load first
        if (pos < limit) {
            this.ch = sql[pos];
        } else {
            this.ch = 0;
        }

        if (!this.recordComments) {
            this.comments = null;
        } else if (this.comments != null) {
            this.comments.clear();
        }

        this.lastToken = null;
        this.tokenCache = null;
        this.tokenCache2 = null;
        this.token = null;

        this.inCStyleComment = false;
        this.offsetCache = this.sizeCache = 0;

        this.paramIndex = 0;
        this.lastTokenIndex = 0;

        this.stringValue = null;
    }

    public String originalStringValue() {
        return new String(sql, offsetCache, sizeCache, charset);
    }

    public String stringValue() {
        return null == stringValue ? stringValue = originalStringValue() : stringValue;
    }

    /**
     * if {@link #stringValue()} returns "'abc\\'d'", then "abc\\'d" is appended
     */
    public void appendStringContent(StringBuilder sb) {
        sb.append(new String(sbuf, 1, sizeCache - 2, charset));
    }

    public String stringValueUppercase() {
        return stringValue().toUpperCase();
    }

    public int paramIndex() {
        return paramIndex;
    }

    public String subSql(final int offset, final int length) {
        return new String(sql, offset, length, charset);
    }

    /**
     * @param token must be a keyword
     */
    public void addCacheToke(MySQLToken token) {
        if (tokenCache != null) {
            tokenCache2 = token;
        } else {
            tokenCache = token;
        }
    }

    public MySQLToken token() {
        if (tokenCache2 != null) {
            return tokenCache2;
        }
        if (tokenCache != null) {
            return tokenCache;
        }
        return token;
    }

    /**
     * {@link #token} must be {@link MySQLToken#LITERAL_NUM_PURE_DIGIT}
     */
    public Number integerValue() {
        // 2147483647
        // 9223372036854775807
        if (sizeCache < 10
            || sizeCache == 10 && (sql[offsetCache] < '2' || sql[offsetCache] == '2' && sql[offsetCache + 1] == '0')) {
            int rst = 0;
            int end = offsetCache + sizeCache;
            for (int i = offsetCache; i < end; ++i) {
                rst = (rst << 3) + (rst << 1);
                rst += sql[i] - '0';
            }
            return rst;
        } else if (sizeCache < 19 || sizeCache == 19 && sql[offsetCache] < '9') {
            long rst = 0;
            int end = offsetCache + sizeCache;
            for (int i = offsetCache; i < end; ++i) {
                rst = (rst << 3) + (rst << 1);
                rst += sql[i] - '0';
            }
            return rst;
        } else {
            return new BigInteger(new String(sql, offsetCache, sizeCache, charset), 10);
        }
    }

    public BigDecimal decimalValue() {
        // QS_TODO [performance enhance]: prevent BigDecimal's parser
        return new BigDecimal(new String(sql, offsetCache, sizeCache, charset));
    }

    private boolean next() throws SQLSyntaxErrorException {
        if (pos + 1 >= limit) {
            // set eof
            pos = limit;
            ch = 0;
            if (inCStyleComment) {
                throw new SQLSyntaxErrorException("unclosed '/*!' comment");
            }
            return false;
        }
        ch = sql[++pos];
        return true;
    }

    private int available() {
        return limit - pos;
    }

    private boolean eof() {
        return pos >= limit;
    }

    private void addComment(int from, int to) {
        if (!recordComments) {
            return;
        }
        if (null == comments) {
            comments = new ArrayList<>();
        }
        final byte[] bytes = new byte[to - from];
        System.arraycopy(sql, from, bytes, 0, to - from);
        comments.add(bytes);
    }

    private void skipSeparator() throws SQLSyntaxErrorException {
        while (!eof()) {
            while (FastCharTypes.isSpace(ch)) {
                if (!next()) {
                    return; // eof
                }
            }

            switch (ch) {
            // # line comment
            case '#': {
                final int from = pos;
                while (true) {
                    if (!next()) {
                        addComment(from, pos);
                        return; // eof
                    }
                    if ('\n' == ch) {
                        final boolean hasNext = next();
                        addComment(from, pos);
                        if (!hasNext) {
                            return; // eof
                        }
                        break;
                    }
                }
            }
            continue;

            case '/': // /* ... */ comment
                if (available() >= 3 && '*' == sql[pos + 1]) {
                    if ('!' == sql[pos + 2]) {
                        inCStyleComment = true;
                        inCStyleCommentIgnore = false;
                        if (available() >= 8 &&
                            FastCharTypes.isDigit(sql[pos + 3]) &&
                            FastCharTypes.isDigit(sql[pos + 4]) &&
                            FastCharTypes.isDigit(sql[pos + 5]) &&
                            FastCharTypes.isDigit(sql[pos + 6]) &&
                            FastCharTypes.isDigit(sql[pos + 7])) {
                            // /*![12345] ... */ MySQL specific markers
                            int version = sql[pos + 3] - '0';
                            version *= 10;
                            version += sql[pos + 4] - '0';
                            version *= 10;
                            version += sql[pos + 5] - '0';
                            version *= 10;
                            version += sql[pos + 6] - '0';
                            version *= 10;
                            version += sql[pos + 7] - '0';
                            if (version > this.version) {
                                inCStyleCommentIgnore = true;
                            }
                            pos += 7; // skip the version
                        } else {
                            pos += 2; // just skip the comment header
                        }
                        if (!next()) {
                            // will throw in next func
                            return; // eof
                        }
                        skipSeparator();
                    } else {
                        // normal comment
                        final int from = pos;
                        ++pos;
                        while (true) {
                            if (!next()) {
                                throw new SQLSyntaxErrorException("unclosed '/*' comment");
                            }
                            if ('*' == ch && available() >= 2 && '/' == sql[pos + 1]) {
                                ++pos;
                                final boolean hasNext = next();
                                addComment(from, pos);
                                if (!hasNext) {
                                    return; // eof
                                }
                                break;
                            }
                        }
                        continue;
                    }
                }
                return;

            case '-': // -- line comment
                if (available() >= 3 && '-' == sql[pos + 1] && FastCharTypes.isSpace(sql[pos + 2])) {
                    final int from = pos;
                    pos += 2;
                    while (true) {
                        if (!next()) {
                            addComment(from, pos);
                            return; // eof
                        }
                        if ('\n' == ch) {
                            final boolean hasNext = next();
                            addComment(from, pos);
                            if (!hasNext) {
                                return; // eof
                            }
                            break;
                        }
                    }
                    continue;
                }
                // fall through

            default:
                return;
            }
        }
    }

    private MySQLToken scanIdentifierFromNumber(int initOffset, int initSize) throws SQLSyntaxErrorException {
        offsetCache = initOffset;
        sizeCache = initSize;
        while (FastCharTypes.isIdentifier(ch)) {
            ++sizeCache;
            if (!next()) {
                break;
            }
        }
        final MySQLToken key = MySQLToken.parseKeyword(sql, offsetCache, sizeCache);
        if (null == key) {
            return MySQLToken.IDENTIFIER;
        }
        stringValue = key.getStr();
        return key;
    }

    /**
     * @param quoteMode if false: first <code>0x</code> has been skipped; if true: first <code>x'</code> has been skipped
     */
    private MySQLToken scanHex(boolean quoteMode) throws SQLSyntaxErrorException {
        offsetCache = pos;
        while (FastCharTypes.isHex(ch) && next())
            ;

        sizeCache = pos - offsetCache;
        if (quoteMode) {
            if (ch != '\'') {
                // and eof will get here
                throw new SQLSyntaxErrorException("unclosed hex");
            }
            next();
        } else if (FastCharTypes.isIdentifier(ch) || 0 == sizeCache) {
            return scanIdentifierFromNumber(offsetCache - 2, sizeCache + 2);
        }
        return MySQLToken.LITERAL_HEX;
    }

    /**
     * @param quoteMode if false: first <code>0b</code> has been skipped; if true: first <code>b'</code> has been skipped
     */
    private MySQLToken scanBitField(boolean quoteMode) throws SQLSyntaxErrorException {
        offsetCache = pos;
        while (('0' == ch || '1' == ch) && next())
            ;

        sizeCache = pos - offsetCache;
        if (quoteMode) {
            if (ch != '\'') {
                // and eof will get here
                throw new SQLSyntaxErrorException("unclosed bit field");
            }
            next();
        } else if (FastCharTypes.isIdentifier(ch) || 0 == sizeCache) {
            return scanIdentifierFromNumber(offsetCache - 2, sizeCache + 2);
        }
        return MySQLToken.LITERAL_BIT;
    }

    /**
     * Append a character to sbuf.
     */
    private void put(char ch, int index) {
        put((byte) ch, index);
    }

    private void put(byte ch, int index) {
        if (index >= sbuf.length) {
            byte[] newsbuf = new byte[sbuf.length * 2];
            System.arraycopy(sbuf, 0, newsbuf, 0, sbuf.length);
            sbuf = newsbuf;
        }
        sbuf[index] = ch;
    }

    private MySQLToken scanString() throws SQLSyntaxErrorException {
        boolean dq = false;
        if (ch != '\'') {
            if (ch == '"') {
                dq = true;
            } else {
                throw new SQLSyntaxErrorException("first char must be \" or '");
            }
        }

        offsetCache = pos;
        int sz = 1;
        sbuf[0] = '\'';
        loop:
        while (true) {
            if (!next()) {
                throw new SQLSyntaxErrorException("unclosed string");
            }
            switch (ch) {
            case '\'':
                if (!dq) {
                    final boolean escape = available() >= 2 && '\'' == sql[pos + 1];
                    next();
                    if (escape) {
                        put(noBackslashEscapes ? '\'' : '\\', sz++);
                        put('\'', sz++);
                        continue;
                    } else {
                        put('\'', sz++);
                        break loop;
                    }
                }
                put(noBackslashEscapes ? '\'' : '\\', sz++);
                put('\'', sz++);
                continue;
            case '"':
                if (dq) {
                    final boolean escape = available() >= 2 && '"' == sql[pos + 1];
                    next();
                    if (escape) {
                        put('"', sz++);
                        continue;
                    } else {
                        put('\'', sz++);
                        break loop;
                    }
                }
                put('"', sz++);
                continue;
            case '\\':
                if (!noBackslashEscapes) {
                    if (!next()) {
                        throw new SQLSyntaxErrorException("unfinished backslash escape");
                    }
                    put('\\', sz++);
                    put(ch, sz++);
                    continue;
                }
            default:
                put(ch, sz++);
            }
        }

        sizeCache = pos - offsetCache;
        stringValue = new String(sbuf, 0, sz, charset);
        return MySQLToken.LITERAL_CHARS;
    }

    /**
     * if first char is <code>.</code>, token may be {@link MySQLToken#PUNC_DOT}
     * if invalid char is presented after <code>.</code>
     */
    private MySQLToken scanNumber() throws SQLSyntaxErrorException {
        offsetCache = pos;
        sizeCache = 1;
        final boolean fstDot = ch == '.';
        boolean dot = fstDot;
        boolean sign = false;
        int state = fstDot ? 1 : 0;

        for (; next(); ++sizeCache) {
            switch (state) {
            case 0:
                if (!FastCharTypes.isDigit(ch)) {
                    if ('.' == ch) {
                        dot = true;
                        state = 1;
                    } else if ('e' == ch || 'E' == ch) {
                        state = 3;
                    } else if (FastCharTypes.isIdentifier(ch)) {
                        return scanIdentifierFromNumber(offsetCache, sizeCache);
                    } else {
                        return MySQLToken.LITERAL_NUM_PURE_DIGIT;
                    }
                }
                break;
            case 1:
                if (FastCharTypes.isDigit(ch)) {
                    state = 2;
                } else if ('e' == ch || 'E' == ch) {
                    state = 3;
                } else if (FastCharTypes.isIdentifier(ch) && fstDot) {
                    sizeCache = 1;
                    ch = sql[pos = offsetCache + 1];
                    return MySQLToken.PUNC_DOT;
                } else {
                    return MySQLToken.LITERAL_NUM_MIX_DIGIT;
                }
                break;
            case 2:
                if (!FastCharTypes.isDigit(ch)) {
                    if ('e' == ch || 'E' == ch) {
                        state = 3;
                    } else if (FastCharTypes.isIdentifier(ch) && fstDot) {
                        sizeCache = 1;
                        ch = sql[pos = offsetCache + 1];
                        return MySQLToken.PUNC_DOT;
                    } else {
                        return MySQLToken.LITERAL_NUM_MIX_DIGIT;
                    }
                }
                break;
            case 3:
                if (FastCharTypes.isDigit(ch)) {
                    state = 5;
                } else if (ch == '+' || ch == '-') {
                    sign = true;
                    state = 4;
                } else if (fstDot) {
                    sizeCache = 1;
                    ch = sql[pos = offsetCache + 1];
                    return MySQLToken.PUNC_DOT;
                } else if (!dot) {
                    if (FastCharTypes.isIdentifier(ch)) {
                        return scanIdentifierFromNumber(offsetCache, sizeCache);
                    } else {
                        final MySQLToken key = MySQLToken.parseKeyword(sql, offsetCache, sizeCache);
                        if (null == key) {
                            return MySQLToken.IDENTIFIER;
                        }
                        stringValue = key.getStr();
                        return key;
                    }
                } else {
                    throw new SQLSyntaxErrorException(
                        "invalid char after '.' and 'e' for as part of number: " + (char) (ch & 0xFF));
                }
                break;
            case 4:
                if (FastCharTypes.isDigit(ch)) {
                    state = 5;
                    break;
                } else if (fstDot) {
                    sizeCache = 1;
                    ch = sql[pos = offsetCache + 1];
                    return MySQLToken.PUNC_DOT;
                } else if (!dot) {
                    ch = sql[--pos];
                    --sizeCache;
                    final MySQLToken key = MySQLToken.parseKeyword(sql, offsetCache, sizeCache);
                    if (null == key) {
                        return MySQLToken.IDENTIFIER;
                    }
                    stringValue = key.getStr();
                    return key;
                } else {
                    throw new SQLSyntaxErrorException("expect digit char after SIGN for 'e': " + (char) (ch & 0xFF));
                }
            case 5:
                if (FastCharTypes.isDigit(ch)) {
                    break;
                } else if (FastCharTypes.isIdentifier(ch)) {
                    if (fstDot) {
                        sizeCache = 1;
                        ch = sql[pos = offsetCache + 1];
                        return MySQLToken.PUNC_DOT;
                    } else if (!dot) {
                        if (sign) {
                            ch = sql[pos = offsetCache];
                            return scanIdentifierFromNumber(pos, 0);
                        } else {
                            return scanIdentifierFromNumber(offsetCache, sizeCache);
                        }
                    }
                }
                return MySQLToken.LITERAL_NUM_MIX_DIGIT;
            }
        }

        switch (state) {
        case 0:
            return MySQLToken.LITERAL_NUM_PURE_DIGIT;
        case 1:
            if (fstDot) {
                return MySQLToken.PUNC_DOT;
            }
        case 2:
        case 5:
            return MySQLToken.LITERAL_NUM_MIX_DIGIT;
        case 3:
            if (fstDot) {
                sizeCache = 1;
                ch = sql[pos = offsetCache + 1];
                return MySQLToken.PUNC_DOT;
            } else if (!dot) {
                final MySQLToken key = MySQLToken.parseKeyword(sql, offsetCache, sizeCache);
                if (null == key) {
                    return MySQLToken.IDENTIFIER;
                }
                stringValue = key.getStr();
                return key;
            } else {
                throw new SQLSyntaxErrorException("expect digit char after SIGN for 'e': " + (char) (ch & 0xFF));
            }
        case 4:
            if (fstDot) {
                sizeCache = 1;
                ch = sql[pos = offsetCache + 1];
                return MySQLToken.PUNC_DOT;
            } else if (!dot) {
                ch = sql[--pos];
                --sizeCache;
                final MySQLToken key = MySQLToken.parseKeyword(sql, offsetCache, sizeCache);
                if (null == key) {
                    return MySQLToken.IDENTIFIER;
                }
                stringValue = key.getStr();
                return key;
            } else {
                throw new SQLSyntaxErrorException("expect digit char after SIGN for 'e': " + (char) (ch & 0xFF));
            }
        default:
            throw new SQLSyntaxErrorException("invalid state when parse number");
        }
    }

    /**
     * not SQL syntax
     */
    private MySQLToken scanPlaceHolder() throws SQLSyntaxErrorException {
        offsetCache = pos;
        sizeCache = 0;
        for (; ch != '}'; ++sizeCache) {
            if (!next()) {
                throw new SQLSyntaxErrorException("unclosed placeholder");
            }
        }
        next();
        return MySQLToken.PLACE_HOLDER;
    }

    /**
     * id is NOT included in <code>`</code>.
     */
    private MySQLToken scanIdentifier() throws SQLSyntaxErrorException {
        if (ch == '$' && available() >= 2 && '{' == sql[pos + 1]) {
            ++pos;
            if (!next()) {
                throw new SQLSyntaxErrorException("unclosed placeholder");
            }
            return scanPlaceHolder();
        }
        return scanIdentifierFromNumber(pos, 0);
    }

    /**
     * first <code>@@</code> is included
     */
    private MySQLToken scanSystemVariable() throws SQLSyntaxErrorException {
        assert available() >= 2 && '@' == ch && '@' == sql[pos + 1];
        ++pos;
        if (!next()) {
            throw new SQLSyntaxErrorException("empty system variable");
        }
        offsetCache = pos;
        sizeCache = 0;
        if (ch == '`') {
            for (++sizeCache; ; ++sizeCache) {
                if (!next()) {
                    throw new SQLSyntaxErrorException("unclosed system variable");
                }
                if ('`' == ch) {
                    ++sizeCache;
                    if (!next() || ch != '`') {
                        break;
                    }
                }
            }
        } else {
            while (FastCharTypes.isIdentifier(ch)) {
                ++sizeCache;
                if (!next()) {
                    break;
                }
            }
        }
        return MySQLToken.SYS_VAR;
    }

    /**
     * first <code>@</code> is included
     */
    private MySQLToken scanUserVariable() throws SQLSyntaxErrorException {
        assert '@' == ch;
        offsetCache = pos;
        sizeCache = 1;
        if (!next()) {
            throw new SQLSyntaxErrorException("empty user variable");
        }

        boolean dq = false;
        switch (ch) {
        case '"':
            dq = true;
        case '\'': {
            loop:
            for (++sizeCache; ; ++sizeCache) {
                if (!next()) {
                    throw new SQLSyntaxErrorException("unclosed user variable");
                }
                switch (ch) {
                case '\\':
                    if (!noBackslashEscapes) {
                        ++sizeCache;
                        if (!next()) {
                            throw new SQLSyntaxErrorException("unfinished backslash escape");
                        }
                    }
                    break;
                case '"':
                    if (dq) {
                        ++sizeCache;
                        final boolean escape = available() >= 2 && '"' == sql[pos + 1];
                        next();
                        if (!escape) {
                            break loop;
                        }
                    }
                    break;
                case '\'':
                    if (!dq) {
                        ++sizeCache;
                        final boolean escape = available() >= 2 && '\'' == sql[pos + 1];
                        next();
                        if (!escape) {
                            break loop;
                        }
                    }
                    break;
                }
            }
        }
        break;
        case '`':
            for (++sizeCache; ; ++sizeCache) {
                if (!next()) {
                    throw new SQLSyntaxErrorException("unclosed system variable");
                }
                if ('`' == ch) {
                    ++sizeCache;
                    if (!next() || ch != '`') {
                        break;
                    }
                }
            }
            break;
        default:
            while (FastCharTypes.isIdentifier(ch) || '.' == ch) {
                ++sizeCache;
                if (!next()) {
                    break;
                }
            }
        }
        return MySQLToken.USR_VAR;
    }

    /**
     * id is included in <code>`</code>. first <code>`</code> is included
     */
    private MySQLToken scanIdentifierWithAccent() throws SQLSyntaxErrorException {
        assert '`' == ch;
        offsetCache = pos;
        while (true) {
            if (!next()) {
                throw new SQLSyntaxErrorException("unclosed identifier");
            }
            if ('`' == ch) {
                if (!next() || ch != '`') {
                    break;
                }
            }
        }
        sizeCache = pos - offsetCache;
        return MySQLToken.IDENTIFIER;
    }

    private MySQLToken nextTokenInternal() throws SQLSyntaxErrorException {
        switch (ch) {
        case '0':
            if (available() >= 2) {
                switch (sql[pos + 1]) {
                case 'x':
                    ++pos;
                    next(); // pure 0x is allowed and treated as an identifier
                    return scanHex(false);
                case 'b':
                    ++pos;
                    next(); // pure 0b is allowed and treated as an identifier
                    return scanBitField(false);
                }
            }
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            return scanNumber();
        case '.':
            if (available() >= 2 && FastCharTypes.isDigit(sql[pos + 1])) {
                return scanNumber();
            } else {
                next();
                return MySQLToken.PUNC_DOT;
            }
        case '\'':
        case '"':
            return scanString();
        case 'n':
        case 'N':
            if (available() >= 2 && '\'' == sql[pos + 1]) {
                next();
                scanString();
                return MySQLToken.LITERAL_NCHARS;
            }
            return scanIdentifier();
        case 'x':
        case 'X':
            if (available() >= 2 && '\'' == sql[pos + 1]) {
                ++pos;
                if (!next()) {
                    throw new SQLSyntaxErrorException("unclosed hex string");
                }
                return scanHex(true);
            }
            return scanIdentifier();
        case 'b':
        case 'B':
            if (available() >= 2 && '\'' == sql[pos + 1]) {
                ++pos;
                if (!next()) {
                    throw new SQLSyntaxErrorException("unclosed bit field");
                }
                return scanBitField(true);
            }
            return scanIdentifier();
        case '@':
            if (available() >= 2 && '@' == sql[pos + 1]) {
                return scanSystemVariable();
            }
            return scanUserVariable();
        case '?':
            next();
            ++paramIndex;
            return MySQLToken.QUESTION_MARK;
        case '(':
            next();
            return MySQLToken.PUNC_LEFT_PAREN;
        case ')':
            next();
            return MySQLToken.PUNC_RIGHT_PAREN;
        case '[':
            next();
            return MySQLToken.PUNC_LEFT_BRACKET;
        case ']':
            next();
            return MySQLToken.PUNC_RIGHT_BRACKET;
        case '{':
            next();
            return MySQLToken.PUNC_LEFT_BRACE;
        case '}':
            next();
            return MySQLToken.PUNC_RIGHT_BRACE;
        case ',':
            next();
            return MySQLToken.PUNC_COMMA;
        case ';':
            next();
            return MySQLToken.PUNC_SEMICOLON;
        case ':':
            if (available() >= 2 && '=' == sql[pos + 1]) {
                ++pos;
                next();
                return MySQLToken.OP_ASSIGN;
            }
            next();
            return MySQLToken.PUNC_COLON;
        case '=':
            next();
            return MySQLToken.OP_EQUALS;
        case '~':
            next();
            return MySQLToken.OP_TILDE;
        case '*':
            if (inCStyleComment && available() >= 2 && '/' == sql[pos + 1]) {
                inCStyleComment = false;
                ++pos;
                next();
                return MySQLToken.PUNC_C_STYLE_COMMENT_END;
            }
            next();
            return MySQLToken.OP_ASTERISK;
        case '-':
            if (available() >= 2 && '>' == sql[pos + 1]) {
                if (available() >= 3 && '>' == sql[pos + 2]) {
                    pos += 2;
                    next();
                    return MySQLToken.OP_JSON_UNQUOTE_EXTRACT;
                } else {
                    ++pos;
                    next();
                    return MySQLToken.OP_JSON_EXTRACT;
                }
            }
            next();
            return MySQLToken.OP_MINUS;
        case '+':
            next();
            return MySQLToken.OP_PLUS;
        case '^':
            next();
            return MySQLToken.OP_CARET;
        case '/':
            next();
            return MySQLToken.OP_SLASH;
        case '%':
            next();
            return MySQLToken.OP_PERCENT;
        case '&':
            if (available() >= 2 && '&' == sql[pos + 1]) {
                ++pos;
                next();
                return MySQLToken.OP_LOGICAL_AND;
            }
            next();
            return MySQLToken.OP_AMPERSAND;
        case '|':
            if (available() >= 2 && '|' == sql[pos + 1]) {
                ++pos;
                next();
                return MySQLToken.OP_LOGICAL_OR;
            }
            next();
            return MySQLToken.OP_VERTICAL_BAR;
        case '!':
            if (available() >= 2 && '=' == sql[pos + 1]) {
                ++pos;
                next();
                return MySQLToken.OP_NOT_EQUALS;
            }
            next();
            return MySQLToken.OP_EXCLAMATION;
        case '>':
            if (available() >= 2) {
                if ('=' == sql[pos + 1]) {
                    ++pos;
                    next();
                    return MySQLToken.OP_GREATER_OR_EQUALS;
                } else if ('>' == sql[pos + 1]) {
                    ++pos;
                    next();
                    return MySQLToken.OP_RIGHT_SHIFT;
                }
            }
            next();
            return MySQLToken.OP_GREATER_THAN;
        case '<':
            if (available() >= 2) {
                switch (sql[pos + 1]) {
                case '=':
                    if (available() >= 3 && '>' == sql[pos + 2]) {
                        pos += 2;
                        next();
                        return MySQLToken.OP_NULL_SAFE_EQUALS;
                    }
                    ++pos;
                    next();
                    return MySQLToken.OP_LESS_OR_EQUALS;
                case '>':
                    ++pos;
                    next();
                    return MySQLToken.OP_LESS_OR_GREATER;
                case '<':
                    ++pos;
                    next();
                    return MySQLToken.OP_LEFT_SHIFT;
                }
            }
            next();
            return MySQLToken.OP_LESS_THAN;
        case '`':
            return scanIdentifierWithAccent();
        case '\0':
            // c string style \0 token, ignore
            if (eof()) {
                return MySQLToken.EOF;
            } else {
                throw new SQLSyntaxErrorException("unsupported character: " + ch);
            }
        default:
            if (FastCharTypes.isIdentifier(ch)) {
                return scanIdentifier();
            } else {
                throw new SQLSyntaxErrorException("unsupported character: " + ch);
            }
        }
    }

    public MySQLToken nextToken() throws SQLSyntaxErrorException {
        // reset cache first
        if (!recordComments) {
            comments = null;
        } else if (comments != null) {
            comments.clear();
        }
        offsetCache = 0;
        sizeCache = 0;
        stringValue = null;

        // record last token
        this.lastTokenIndex = pos;

        if (tokenCache2 != null) {
            tokenCache2 = null;
            return tokenCache;
        }
        if (tokenCache != null) {
            tokenCache = null;
            return token;
        }

        do {
            skipSeparator();
            lastToken = token;
            token = nextTokenInternal();
        } while ((inCStyleComment && inCStyleCommentIgnore) || MySQLToken.PUNC_C_STYLE_COMMENT_END == token);
        return token;
    }
}
