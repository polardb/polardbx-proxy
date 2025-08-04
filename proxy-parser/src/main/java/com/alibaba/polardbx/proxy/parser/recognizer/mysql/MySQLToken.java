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

package com.alibaba.polardbx.proxy.parser.recognizer.mysql;

import com.alibaba.polardbx.proxy.parser.util.FnvHash;
import lombok.Getter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public enum MySQLToken {
    EOF(null),

    IDENTIFIER(null), // with or without ``, original string in sql
    LITERAL_HEX(null), // without 0x or x''
    LITERAL_BIT(null), // without 0b or b''
    LITERAL_NUM_PURE_DIGIT(null),
    LITERAL_NUM_MIX_DIGIT(null),
    LITERAL_CHARS(null), // with '' or "", original string in sql
    LITERAL_NCHARS(null), // without n/N, with '' or "", original string in sql
    PLACE_HOLDER(null), // without ${}, only string in it
    SYS_VAR(null), // without @@
    USR_VAR(null), // with @

    LITERAL_NULL("NULL"),
    LITERAL_BOOL_FALSE("FALSE"),
    LITERAL_BOOL_TRUE("TRUE"),

    /**
     * ?
     */
    QUESTION_MARK,
    /**
     * (
     */
    PUNC_LEFT_PAREN,
    /**
     * )
     */
    PUNC_RIGHT_PAREN,
    /**
     * {
     */
    PUNC_LEFT_BRACE,
    /**
     * }
     */
    PUNC_RIGHT_BRACE,
    /**
     * [
     */
    PUNC_LEFT_BRACKET,
    /**
     * ]
     */
    PUNC_RIGHT_BRACKET,
    /**
     * ;
     */
    PUNC_SEMICOLON,
    /**
     * ,
     */
    PUNC_COMMA,
    /**
     * .
     */
    PUNC_DOT,
    /**
     * :
     */
    PUNC_COLON,
    /**
     * <code>*</code><code>/</code>
     */
    PUNC_C_STYLE_COMMENT_END,

    /**
     * =
     */
    OP_EQUALS,
    /**
     * >
     */
    OP_GREATER_THAN,
    /**
     * <
     */
    OP_LESS_THAN,
    /**
     * !
     */
    OP_EXCLAMATION,
    /**
     * ~
     */
    OP_TILDE,
    /**
     * +
     */
    OP_PLUS,
    /**
     * -
     */
    OP_MINUS,
    /**
     *
     */
    OP_ASTERISK,
    /**
     * /
     */
    OP_SLASH,
    /**
     * &
     */
    OP_AMPERSAND,
    /**
     * |
     */
    OP_VERTICAL_BAR,
    /**
     * ^
     */
    OP_CARET,
    /**
     * %
     */
    OP_PERCENT,
    /**
     * :=
     */
    OP_ASSIGN,
    /**
     * <=
     */
    OP_LESS_OR_EQUALS,
    /**
     * <>
     */
    OP_LESS_OR_GREATER,
    /**
     * >=
     */
    OP_GREATER_OR_EQUALS,
    /**
     * !=
     */
    OP_NOT_EQUALS,
    /**
     * &&
     */
    OP_LOGICAL_AND,
    /**
     * ->>
     */
    OP_JSON_UNQUOTE_EXTRACT,
    /**
     * ->
     */
    OP_JSON_EXTRACT,
    /**
     * ||
     */
    OP_LOGICAL_OR,
    /**
     * <<
     */
    OP_LEFT_SHIFT,
    /**
     * >>
     */
    OP_RIGHT_SHIFT,
    /**
     * <=>
     */
    OP_NULL_SAFE_EQUALS,

    /**
     * Keywords.
     * <a href="https://dev.mysql.com/doc/refman/8.0/en/keywords.html">MySQL keywords</a>
     */
    // all MySQL 8.0 keyword
    KW_ACCESSIBLE,
    KW_ACCOUNT,
    KW_ACTION,
    KW_ACTIVE,
    KW_ADD,
    KW_ADMIN,
    KW_AFTER,
    KW_AGAINST,
    KW_AGGREGATE,
    KW_ALGORITHM,
    KW_ALL,
    KW_ALTER,
    KW_ALWAYS,
    KW_ANALYSE,
    KW_ANALYZE,
    KW_AND,
    KW_ANY,
    KW_ARRAY,
    KW_AS,
    KW_ASC,
    KW_ASCII,
    KW_ASENSITIVE,
    KW_AT,
    KW_ATTRIBUTE,
    KW_AUTHENTICATION,
    KW_AUTOEXTEND_SIZE,
    KW_AUTO_INCREMENT,
    KW_AVG,
    KW_AVG_ROW_LENGTH,
    KW_BACKUP,
    KW_BEFORE,
    KW_BEGIN,
    KW_BETWEEN,
    KW_BIGINT,
    KW_BINARY,
    KW_BINLOG,
    KW_BIT,
    KW_BLOB,
    KW_BLOCK,
    KW_BOOL,
    KW_BOOLEAN,
    KW_BOTH,
    KW_BTREE,
    KW_BUCKETS,
    KW_BULK,
    KW_BY,
    KW_BYTE,
    KW_CACHE,
    KW_CALL,
    KW_CASCADE,
    KW_CASCADED,
    KW_CASE,
    KW_CATALOG_NAME,
    KW_CHAIN,
    KW_CHALLENGE_RESPONSE,
    KW_CHANGE,
    KW_CHANGED,
    KW_CHANNEL,
    KW_CHAR,
    KW_CHARACTER,
    KW_CHARSET,
    KW_CHECK,
    KW_CHECKSUM,
    KW_CIPHER,
    KW_CLASS_ORIGIN,
    KW_CLIENT,
    KW_CLONE,
    KW_CLOSE,
    KW_COALESCE,
    KW_CODE,
    KW_COLLATE,
    KW_COLLATION,
    KW_COLUMN,
    KW_COLUMNS,
    KW_COLUMN_FORMAT,
    KW_COLUMN_NAME,
    KW_COMMENT,
    KW_COMMIT,
    KW_COMMITTED,
    KW_COMPACT,
    KW_COMPLETION,
    KW_COMPONENT,
    KW_COMPRESSED,
    KW_COMPRESSION,
    KW_CONCURRENT,
    KW_CONDITION,
    KW_CONNECTION,
    KW_CONSISTENT,
    KW_CONSTRAINT,
    KW_CONSTRAINT_CATALOG,
    KW_CONSTRAINT_NAME,
    KW_CONSTRAINT_SCHEMA,
    KW_CONTAINS,
    KW_CONTEXT,
    KW_CONTINUE,
    KW_CONVERT,
    KW_CPU,
    KW_CREATE,
    KW_CROSS,
    KW_CUBE,
    KW_CUME_DIST,
    KW_CURRENT,
    KW_CURRENT_DATE,
    KW_CURRENT_TIME,
    KW_CURRENT_TIMESTAMP,
    KW_CURRENT_USER,
    KW_CURSOR,
    KW_CURSOR_NAME,
    KW_DATA,
    KW_DATABASE,
    KW_DATABASES,
    KW_DATAFILE,
    KW_DATE,
    KW_DATETIME,
    KW_DAY,
    KW_DAY_HOUR,
    KW_DAY_MICROSECOND,
    KW_DAY_MINUTE,
    KW_DAY_SECOND,
    KW_DEALLOCATE,
    KW_DEC,
    KW_DECIMAL,
    KW_DECLARE,
    KW_DEFAULT,
    KW_DEFAULT_AUTH,
    KW_DEFINER,
    KW_DEFINITION,
    KW_DELAYED,
    KW_DELAY_KEY_WRITE,
    KW_DELETE,
    KW_DENSE_RANK,
    KW_DESC,
    KW_DESCRIBE,
    KW_DESCRIPTION,
    KW_DES_KEY_FILE,
    KW_DETERMINISTIC,
    KW_DIAGNOSTICS,
    KW_DIRECTORY,
    KW_DISABLE,
    KW_DISCARD,
    KW_DISK,
    KW_DISTINCT,
    KW_DISTINCTROW,
    KW_DIV,
    KW_DO,
    KW_DOUBLE,
    KW_DROP,
    KW_DUAL,
    KW_DUMPFILE,
    KW_DUPLICATE,
    KW_DYNAMIC,
    KW_EACH,
    KW_ELSE,
    KW_ELSEIF,
    KW_EMPTY,
    KW_ENABLE,
    KW_ENCLOSED,
    KW_ENCRYPTION,
    KW_END,
    KW_ENDS,
    KW_ENFORCED,
    KW_ENGINE,
    KW_ENGINES,
    KW_ENGINE_ATTRIBUTE,
    KW_ENUM,
    KW_ERROR,
    KW_ERRORS,
    KW_ESCAPE,
    KW_ESCAPED,
    KW_EVENT,
    KW_EVENTS,
    KW_EVERY,
    KW_EXCEPT,
    KW_EXCHANGE,
    KW_EXCLUDE,
    KW_EXECUTE,
    KW_EXISTS,
    KW_EXIT,
    KW_EXPANSION,
    KW_EXPIRE,
    KW_EXPLAIN,
    KW_EXPORT,
    KW_EXTENDED,
    KW_EXTENT_SIZE,
    KW_FACTOR,
    KW_FAILED_LOGIN_ATTEMPTS,
    KW_FALSE,
    KW_FAST,
    KW_FAULTS,
    KW_FETCH,
    KW_FIELDS,
    KW_FILE,
    KW_FILE_BLOCK_SIZE,
    KW_FILTER,
    KW_FINISH,
    KW_FIRST,
    KW_FIRST_VALUE,
    KW_FIXED,
    KW_FLOAT,
    KW_FLOAT4,
    KW_FLOAT8,
    KW_FLUSH,
    KW_FOLLOWING,
    KW_FOLLOWS,
    KW_FOR,
    KW_FORCE,
    KW_FOREIGN,
    KW_FORMAT,
    KW_FOUND,
    KW_FROM,
    KW_FULL,
    KW_FULLTEXT,
    KW_FUNCTION,
    KW_GENERAL,
    KW_GENERATE,
    KW_GENERATED,
    KW_GEOMCOLLECTION,
    KW_GEOMETRY,
    KW_GEOMETRYCOLLECTION,
    KW_GET,
    KW_GET_FORMAT,
    KW_GET_MASTER_PUBLIC_KEY,
    KW_GET_SOURCE_PUBLIC_KEY,
    KW_GLOBAL,
    KW_GRANT,
    KW_GRANTS,
    KW_GROUP,
    KW_GROUPING,
    KW_GROUPS,
    KW_GROUP_REPLICATION,
    KW_GTID_ONLY,
    KW_HANDLER,
    KW_HASH,
    KW_HAVING,
    KW_HELP,
    KW_HIGH_PRIORITY,
    KW_HISTOGRAM,
    KW_HISTORY,
    KW_HOST,
    KW_HOSTS,
    KW_HOUR,
    KW_HOUR_MICROSECOND,
    KW_HOUR_MINUTE,
    KW_HOUR_SECOND,
    KW_IDENTIFIED,
    KW_IF,
    KW_IGNORE,
    KW_IGNORE_SERVER_IDS,
    KW_IMPORT,
    KW_IN,
    KW_INACTIVE,
    KW_INDEX,
    KW_INDEXES,
    KW_INFILE,
    KW_INITIAL,
    KW_INITIAL_SIZE,
    KW_INITIATE,
    KW_INNER,
    KW_INOUT,
    KW_INSENSITIVE,
    KW_INSERT,
    KW_INSERT_METHOD,
    KW_INSTALL,
    KW_INSTANCE,
    KW_INT,
    KW_INT1,
    KW_INT2,
    KW_INT3,
    KW_INT4,
    KW_INT8,
    KW_INTEGER,
    KW_INTERSECT,
    KW_INTERVAL,
    KW_INTO,
    KW_INVISIBLE,
    KW_INVOKER,
    KW_IO,
    KW_IO_AFTER_GTIDS,
    KW_IO_BEFORE_GTIDS,
    KW_IO_THREAD,
    KW_IPC,
    KW_IS,
    KW_ISOLATION,
    KW_ISSUER,
    KW_ITERATE,
    KW_JOIN,
    KW_JSON,
    KW_JSON_TABLE,
    KW_JSON_VALUE,
    KW_KEY,
    KW_KEYRING,
    KW_KEYS,
    KW_KEY_BLOCK_SIZE,
    KW_KILL,
    KW_LAG,
    KW_LANGUAGE,
    KW_LAST,
    KW_LAST_VALUE,
    KW_LATERAL,
    KW_LEAD,
    KW_LEADING,
    KW_LEAVE,
    KW_LEAVES,
    KW_LEFT,
    KW_LESS,
    KW_LEVEL,
    KW_LIKE,
    KW_LIMIT,
    KW_LINEAR,
    KW_LINES,
    KW_LINESTRING,
    KW_LIST,
    KW_LOAD,
    KW_LOCAL,
    KW_LOCALTIME,
    KW_LOCALTIMESTAMP,
    KW_LOCK,
    KW_LOCKED,
    KW_LOCKS,
    KW_LOGFILE,
    KW_LOGS,
    KW_LONG,
    KW_LONGBLOB,
    KW_LONGTEXT,
    KW_LOOP,
    KW_LOW_PRIORITY,
    KW_MASTER,
    KW_MASTER_AUTO_POSITION,
    KW_MASTER_BIND,
    KW_MASTER_COMPRESSION_ALGORITHMS,
    KW_MASTER_CONNECT_RETRY,
    KW_MASTER_DELAY,
    KW_MASTER_HEARTBEAT_PERIOD,
    KW_MASTER_HOST,
    KW_MASTER_LOG_FILE,
    KW_MASTER_LOG_POS,
    KW_MASTER_PASSWORD,
    KW_MASTER_PORT,
    KW_MASTER_PUBLIC_KEY_PATH,
    KW_MASTER_RETRY_COUNT,
    KW_MASTER_SERVER_ID,
    KW_MASTER_SSL,
    KW_MASTER_SSL_CA,
    KW_MASTER_SSL_CAPATH,
    KW_MASTER_SSL_CERT,
    KW_MASTER_SSL_CIPHER,
    KW_MASTER_SSL_CRL,
    KW_MASTER_SSL_CRLPATH,
    KW_MASTER_SSL_KEY,
    KW_MASTER_SSL_VERIFY_SERVER_CERT,
    KW_MASTER_TLS_CIPHERSUITES,
    KW_MASTER_TLS_VERSION,
    KW_MASTER_USER,
    KW_MASTER_ZSTD_COMPRESSION_LEVEL,
    KW_MATCH,
    KW_MAXVALUE,
    KW_MAX_CONNECTIONS_PER_HOUR,
    KW_MAX_QUERIES_PER_HOUR,
    KW_MAX_ROWS,
    KW_MAX_SIZE,
    KW_MAX_UPDATES_PER_HOUR,
    KW_MAX_USER_CONNECTIONS,
    KW_MEDIUM,
    KW_MEDIUMBLOB,
    KW_MEDIUMINT,
    KW_MEDIUMTEXT,
    KW_MEMBER,
    KW_MEMORY,
    KW_MERGE,
    KW_MESSAGE_TEXT,
    KW_MICROSECOND,
    KW_MIDDLEINT,
    KW_MIGRATE,
    KW_MINUTE,
    KW_MINUTE_MICROSECOND,
    KW_MINUTE_SECOND,
    KW_MIN_ROWS,
    KW_MOD,
    KW_MODE,
    KW_MODIFIES,
    KW_MODIFY,
    KW_MONTH,
    KW_MULTILINESTRING,
    KW_MULTIPOINT,
    KW_MULTIPOLYGON,
    KW_MUTEX,
    KW_MYSQL_ERRNO,
    KW_NAME,
    KW_NAMES,
    KW_NATIONAL,
    KW_NATURAL,
    KW_NCHAR,
    KW_NDB,
    KW_NDBCLUSTER,
    KW_NESTED,
    KW_NETWORK_NAMESPACE,
    KW_NEVER,
    KW_NEW,
    KW_NEXT,
    KW_NO,
    KW_NODEGROUP,
    KW_NONE,
    KW_NOT,
    KW_NOWAIT,
    KW_NO_WAIT,
    KW_NO_WRITE_TO_BINLOG,
    KW_NTH_VALUE,
    KW_NTILE,
    KW_NULL,
    KW_NULLS,
    KW_NUMBER,
    KW_NUMERIC,
    KW_NVARCHAR,
    KW_OF,
    KW_OFF,
    KW_OFFSET,
    KW_OJ,
    KW_OLD,
    KW_ON,
    KW_ONE,
    KW_ONLY,
    KW_OPEN,
    KW_OPTIMIZE,
    KW_OPTIMIZER_COSTS,
    KW_OPTION,
    KW_OPTIONAL,
    KW_OPTIONALLY,
    KW_OPTIONS,
    KW_OR,
    KW_ORDER,
    KW_ORDINALITY,
    KW_ORGANIZATION,
    KW_OTHERS,
    KW_OUT,
    KW_OUTER,
    KW_OUTFILE,
    KW_OVER,
    KW_OWNER,
    KW_PACK_KEYS,
    KW_PAGE,
    KW_PARSER,
    KW_PARTIAL,
    KW_PARTITION,
    KW_PARTITIONING,
    KW_PARTITIONS,
    KW_PASSWORD,
    KW_PASSWORD_LOCK_TIME,
    KW_PATH,
    KW_PERCENT_RANK,
    KW_PERSIST,
    KW_PERSIST_ONLY,
    KW_PHASE,
    KW_PLUGIN,
    KW_PLUGINS,
    KW_PLUGIN_DIR,
    KW_POINT,
    KW_POLYGON,
    KW_PORT,
    KW_PRECEDES,
    KW_PRECEDING,
    KW_PRECISION,
    KW_PREPARE,
    KW_PRESERVE,
    KW_PREV,
    KW_PRIMARY,
    KW_PRIVILEGES,
    KW_PRIVILEGE_CHECKS_USER,
    KW_PROCEDURE,
    KW_PROCESS,
    KW_PROCESSLIST,
    KW_PROFILE,
    KW_PROFILES,
    KW_PROXY,
    KW_PURGE,
    KW_QUARTER,
    KW_QUERY,
    KW_QUICK,
    KW_RANDOM,
    KW_RANGE,
    KW_RANK,
    KW_READ,
    KW_READS,
    KW_READ_ONLY,
    KW_READ_WRITE,
    KW_REAL,
    KW_REBUILD,
    KW_RECOVER,
    KW_RECURSIVE,
    KW_REDOFILE,
    KW_REDO_BUFFER_SIZE,
    KW_REDUNDANT,
    KW_REFERENCE,
    KW_REFERENCES,
    KW_REGEXP,
    KW_REGISTRATION,
    KW_RELAY,
    KW_RELAYLOG,
    KW_RELAY_LOG_FILE,
    KW_RELAY_LOG_POS,
    KW_RELAY_THREAD,
    KW_RELEASE,
    KW_RELOAD,
    KW_REMOTE,
    KW_REMOVE,
    KW_RENAME,
    KW_REORGANIZE,
    KW_REPAIR,
    KW_REPEAT,
    KW_REPEATABLE,
    KW_REPLACE,
    KW_REPLICA,
    KW_REPLICAS,
    KW_REPLICATE_DO_DB,
    KW_REPLICATE_DO_TABLE,
    KW_REPLICATE_IGNORE_DB,
    KW_REPLICATE_IGNORE_TABLE,
    KW_REPLICATE_REWRITE_DB,
    KW_REPLICATE_WILD_DO_TABLE,
    KW_REPLICATE_WILD_IGNORE_TABLE,
    KW_REPLICATION,
    KW_REQUIRE,
    KW_REQUIRE_ROW_FORMAT,
    KW_RESET,
    KW_RESIGNAL,
    KW_RESOURCE,
    KW_RESPECT,
    KW_RESTART,
    KW_RESTORE,
    KW_RESTRICT,
    KW_RESUME,
    KW_RETAIN,
    KW_RETURN,
    KW_RETURNED_SQLSTATE,
    KW_RETURNING,
    KW_RETURNS,
    KW_REUSE,
    KW_REVERSE,
    KW_REVOKE,
    KW_RIGHT,
    KW_RLIKE,
    KW_ROLE,
    KW_ROLLBACK,
    KW_ROLLUP,
    KW_ROTATE,
    KW_ROUTINE,
    KW_ROW,
    KW_ROWS,
    KW_ROW_COUNT,
    KW_ROW_FORMAT,
    KW_ROW_NUMBER,
    KW_RTREE,
    KW_SAVEPOINT,
    KW_SCHEDULE,
    KW_SCHEMA,
    KW_SCHEMAS,
    KW_SCHEMA_NAME,
    KW_SECOND,
    KW_SECONDARY,
    KW_SECONDARY_ENGINE,
    KW_SECONDARY_ENGINE_ATTRIBUTE,
    KW_SECONDARY_LOAD,
    KW_SECONDARY_UNLOAD,
    KW_SECOND_MICROSECOND,
    KW_SECURITY,
    KW_SELECT,
    KW_SENSITIVE,
    KW_SEPARATOR,
    KW_SERIAL,
    KW_SERIALIZABLE,
    KW_SERVER,
    KW_SESSION,
    KW_SET,
    KW_SHARE,
    KW_SHOW,
    KW_SHUTDOWN,
    KW_SIGNAL,
    KW_SIGNED,
    KW_SIMPLE,
    KW_SKIP,
    KW_SLAVE,
    KW_SLOW,
    KW_SMALLINT,
    KW_SNAPSHOT,
    KW_SOCKET,
    KW_SOME,
    KW_SONAME,
    KW_SOUNDS,
    KW_SOURCE,
    KW_SOURCE_AUTO_POSITION,
    KW_SOURCE_BIND,
    KW_SOURCE_COMPRESSION_ALGORITHMS,
    KW_SOURCE_CONNECT_RETRY,
    KW_SOURCE_DELAY,
    KW_SOURCE_HEARTBEAT_PERIOD,
    KW_SOURCE_HOST,
    KW_SOURCE_LOG_FILE,
    KW_SOURCE_LOG_POS,
    KW_SOURCE_PASSWORD,
    KW_SOURCE_PORT,
    KW_SOURCE_PUBLIC_KEY_PATH,
    KW_SOURCE_RETRY_COUNT,
    KW_SOURCE_SSL,
    KW_SOURCE_SSL_CA,
    KW_SOURCE_SSL_CAPATH,
    KW_SOURCE_SSL_CERT,
    KW_SOURCE_SSL_CIPHER,
    KW_SOURCE_SSL_CRL,
    KW_SOURCE_SSL_CRLPATH,
    KW_SOURCE_SSL_KEY,
    KW_SOURCE_SSL_VERIFY_SERVER_CERT,
    KW_SOURCE_TLS_CIPHERSUITES,
    KW_SOURCE_TLS_VERSION,
    KW_SOURCE_USER,
    KW_SOURCE_ZSTD_COMPRESSION_LEVEL,
    KW_SPATIAL,
    KW_SPECIFIC,
    KW_SQL,
    KW_SQLEXCEPTION,
    KW_SQLSTATE,
    KW_SQLWARNING,
    KW_SQL_AFTER_GTIDS,
    KW_SQL_AFTER_MTS_GAPS,
    KW_SQL_BEFORE_GTIDS,
    KW_SQL_BIG_RESULT,
    KW_SQL_BUFFER_RESULT,
    KW_SQL_CACHE,
    KW_SQL_CALC_FOUND_ROWS,
    KW_SQL_NO_CACHE,
    KW_SQL_SMALL_RESULT,
    KW_SQL_THREAD,
    KW_SQL_TSI_DAY,
    KW_SQL_TSI_HOUR,
    KW_SQL_TSI_MINUTE,
    KW_SQL_TSI_MONTH,
    KW_SQL_TSI_QUARTER,
    KW_SQL_TSI_SECOND,
    KW_SQL_TSI_WEEK,
    KW_SQL_TSI_YEAR,
    KW_SRID,
    KW_SSL,
    KW_STACKED,
    KW_START,
    KW_STARTING,
    KW_STARTS,
    KW_STATS_AUTO_RECALC,
    KW_STATS_PERSISTENT,
    KW_STATS_SAMPLE_PAGES,
    KW_STATUS,
    KW_STOP,
    KW_STORAGE,
    KW_STORED,
    KW_STRAIGHT_JOIN,
    KW_STREAM,
    KW_STRING,
    KW_SUBCLASS_ORIGIN,
    KW_SUBJECT,
    KW_SUBPARTITION,
    KW_SUBPARTITIONS,
    KW_SUPER,
    KW_SUSPEND,
    KW_SWAPS,
    KW_SWITCHES,
    KW_SYSTEM,
    KW_TABLE,
    KW_TABLES,
    KW_TABLESPACE,
    KW_TABLE_CHECKSUM,
    KW_TABLE_NAME,
    KW_TEMPORARY,
    KW_TEMPTABLE,
    KW_TERMINATED,
    KW_TEXT,
    KW_THAN,
    KW_THEN,
    KW_THREAD_PRIORITY,
    KW_TIES,
    KW_TIME,
    KW_TIMESTAMP,
    KW_TIMESTAMPADD,
    KW_TIMESTAMPDIFF,
    KW_TINYBLOB,
    KW_TINYINT,
    KW_TINYTEXT,
    KW_TLS,
    KW_TO,
    KW_TRAILING,
    KW_TRANSACTION,
    KW_TRIGGER,
    KW_TRIGGERS,
    KW_TRUE,
    KW_TRUNCATE,
    KW_TYPE,
    KW_TYPES,
    KW_UNBOUNDED,
    KW_UNCOMMITTED,
    KW_UNDEFINED,
    KW_UNDO,
    KW_UNDOFILE,
    KW_UNDO_BUFFER_SIZE,
    KW_UNICODE,
    KW_UNINSTALL,
    KW_UNION,
    KW_UNIQUE,
    KW_UNKNOWN,
    KW_UNLOCK,
    KW_UNREGISTER,
    KW_UNSIGNED,
    KW_UNTIL,
    KW_UPDATE,
    KW_UPGRADE,
    KW_URL,
    KW_USAGE,
    KW_USE,
    KW_USER,
    KW_USER_RESOURCES,
    KW_USE_FRM,
    KW_USING,
    KW_UTC_DATE,
    KW_UTC_TIME,
    KW_UTC_TIMESTAMP,
    KW_VALIDATION,
    KW_VALUE,
    KW_VALUES,
    KW_VARBINARY,
    KW_VARCHAR,
    KW_VARCHARACTER,
    KW_VARIABLES,
    KW_VARYING,
    KW_VCPU,
    KW_VIEW,
    KW_VIRTUAL,
    KW_VISIBLE,
    KW_WAIT,
    KW_WARNINGS,
    KW_WEEK,
    KW_WEIGHT_STRING,
    KW_WHEN,
    KW_WHERE,
    KW_WHILE,
    KW_WINDOW,
    KW_WITH,
    KW_WITHOUT,
    KW_WORK,
    KW_WRAPPER,
    KW_WRITE,
    KW_X509,
    KW_XA,
    KW_XID,
    KW_XML,
    KW_XOR,
    KW_YEAR,
    KW_YEAR_MONTH,
    KW_ZEROFILL,
    KW_ZONE,
    // and removed since 8.0
    KW_PARSE_GCOL_EXPR;

    @Getter
    private final String str;
    private final byte[] keyword; // keyword in lower case in bytes
    private final long fnvhash; // fnv1a_64_lower hash of keyword

    MySQLToken() {
        final String name = name();
        if (name.startsWith("KW_")) {
            this.str = name.substring(3);
        } else {
            this.str = name;
        }
        this.keyword = this.str.toLowerCase().getBytes(StandardCharsets.UTF_8);
        this.fnvhash = FnvHash.fnv1a_64_lower(keyword, 0, keyword.length);
    }

    MySQLToken(String str) {
        this.str = str;
        if (null == str) {
            this.keyword = null;
            this.fnvhash = 0;
        } else {
            this.keyword = this.str.toLowerCase().getBytes(StandardCharsets.UTF_8);
            this.fnvhash = FnvHash.fnv1a_64_lower(keyword, 0, keyword.length);
        }
    }

    private static final Map<Long, MySQLToken> keywords = new HashMap<>(); // <fnvhash, keyword>

    static {
        for (final MySQLToken keyword : MySQLToken.values()) {
            if (!keyword.name().startsWith("KW_")) {
                continue;
            }
            keywords.put(keyword.fnvhash, keyword);
        }
        keywords.put(LITERAL_NULL.fnvhash, LITERAL_NULL);
        keywords.put(LITERAL_BOOL_TRUE.fnvhash, LITERAL_BOOL_TRUE);
        keywords.put(LITERAL_BOOL_FALSE.fnvhash, LITERAL_BOOL_FALSE);
    }

    public static MySQLToken parseKeyword(final byte[] bytes, final int offset, final int len) {
        final long fnvhash = FnvHash.fnv1a_64_lower(bytes, offset, len);
        final MySQLToken keyword = keywords.get(fnvhash);
        if (null == keyword) {
            return null;
        }
        if (keyword.keyword.length != len) {
            return null;
        }
        for (int i = 0; i < len; ++i) {
            byte ch = bytes[offset + i];
            if (ch >= 'A' && ch <= 'Z') {
                ch = (byte) (ch + 32);
            }
            if (keyword.keyword[i] != ch) {
                return null;
            }
        }
        return keyword;
    }
}
