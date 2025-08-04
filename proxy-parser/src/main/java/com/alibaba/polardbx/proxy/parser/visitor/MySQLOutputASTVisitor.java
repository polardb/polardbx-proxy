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
 * (created at 2011-6-1)
 */
package com.alibaba.polardbx.proxy.parser.visitor;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.ast.expression.PolyadicOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.TernaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.UnaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.BetweenAndExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionEqualsExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionNullSafeEqualsExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.InExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.logical.LogicalAndExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.logical.LogicalOrExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.misc.InExpressionList;
import com.alibaba.polardbx.proxy.parser.ast.expression.misc.QueryExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.misc.UserExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.CaseWhenOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.DefaultValue;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.Identifier;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.JsonExtractExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.MatchExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ParamMarker;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.PlaceHolder;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.RowExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.SysVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.UsrDefVarPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.VariableExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddColumn;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddColumns;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddForeignKey;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddFullTextIndex;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddIndex;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddPartitition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddPrimaryKey;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddSpatialIndex;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AddUniqueKey;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.Algorithm;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AlterColumnDefaultVal;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AlterSpecification;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.AnalyzePartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ChangeColumn;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.CharacterSet;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.CheckPartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.CoalescePartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ConvertCharset;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DBPartitionBy;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DBPartitionOptions;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DropColumn;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DropForeignKey;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DropIndex;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DropPartitition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.DropPrimaryKey;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.EnableKeys;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ExchangePartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ForceOperation;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ImportTablespace;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.LockOperation;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ModifyColumn;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.OptimizePartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.Orderby;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.PartitionBy;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.PartitionByType;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.PartitionDefinition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.PartitionOptions;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.RebuildPartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.RemovePartitioning;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.RenameOperation;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.ReorganizePartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.RepairPartition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.SubPartitionBy;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.SubpartitionDefinition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.TBPartitionBy;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ddl.TruncatePartitition;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.FunctionExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.cast.Cast;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.cast.Convert;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.datetime.Extract;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.datetime.GetFormat;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.datetime.Timestampadd;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.datetime.Timestampdiff;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.groupby.Avg;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.groupby.Count;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.groupby.GroupConcat;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.groupby.Max;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.groupby.Min;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.groupby.Sum;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.string.Char;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.function.string.Trim;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.IntervalPrimary;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralBitField;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralBoolean;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralHexadecimal;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNull;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.polardbx.proxy.parser.ast.expression.string.LikeExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.type.CollateExpression;
import com.alibaba.polardbx.proxy.parser.ast.fragment.GroupBy;
import com.alibaba.polardbx.proxy.parser.ast.fragment.Limit;
import com.alibaba.polardbx.proxy.parser.ast.fragment.LockReference;
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.ast.fragment.SortOrder;
import com.alibaba.polardbx.proxy.parser.ast.fragment.VariableScope;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.ColumnDefinition;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.ColumnDefinition.SpecialIndex;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.TableOptions;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.datatype.DataType;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.datatype.DataType.DataTypeName;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.IndexColumnName;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.IndexDefinition;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.IndexOption;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.ReferenceDefinition;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.index.ReferenceOption;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.Dual;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.IndexHint;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.InnerJoin;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.NaturalJoin;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.OuterJoin;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.StraightJoin;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.SubqueryFactor;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.TableReference;
import com.alibaba.polardbx.proxy.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ChangeRuleVersionStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.CheckTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ClearSeqCacheStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALAnalyzeTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALDeallocateStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALExecuteStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALPrepareStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetCharacterSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetNamesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetSimpleStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DALSetStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.DisableOutlineStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.EnableOutlineStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.InspectGroupSeqRangeStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.InspectRuleVersionStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.Kill;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ReleaseDbLock;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ResyncLocalRulesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ResyncOutlineStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowAuthors;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowBackend;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowBinLogEvent;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowBinaryLog;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowBroadcasts;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowCharaterSet;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowCluster;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowCollation;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowContributors;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowDS;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowDataSources;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowDatabases;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowDbLock;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowDbStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowDdlStatusStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowEngine;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowEngines;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowErrors;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowEvents;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowFrontend;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowFunctionCode;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowFunctionStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowGrantsStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowHtc;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowIndex;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowInstanceType;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowMasterStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowOpenTables;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowOutlines;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowPartitions;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowPlugins;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowPrivileges;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProcedureCode;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProcedureStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProcesslist;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProfile;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProfiles;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowProperties;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRO;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRW;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowReactor;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRule;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowRuleStatusStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowSlaveHosts;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowSlaveStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowSlow;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowStats;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowStc;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowTableStatus;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowTables;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowTopology;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowTrace;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowTriggers;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowVariables;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dal.ShowWarnings;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.CreateOutlineStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLAlterTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLCreateIndexStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLCreateTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLDropIndexStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLDropTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLRenameTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DDLTruncateStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DescTableStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.DropOutlineStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.LockTablesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.ddl.UnLockTablesStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLCallStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLLoadStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectFromUpdateStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectFromUpdateStatement.SelectFromUpdateOption;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLSelectUnionStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.extension.ExtDDLCreatePolicy;
import com.alibaba.polardbx.proxy.parser.ast.stmt.extension.ExtDDLDropPolicy;
import com.alibaba.polardbx.proxy.parser.ast.stmt.mts.MTSReleaseStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.mts.MTSRollbackStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.mts.MTSSavepointStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.mts.MTSSetTransactionStatement;
import com.alibaba.polardbx.proxy.parser.ast.stmt.reload.ReloadDataSources;
import com.alibaba.polardbx.proxy.parser.ast.stmt.reload.ReloadFireworks;
import com.alibaba.polardbx.proxy.parser.ast.stmt.reload.ReloadSchema;
import com.alibaba.polardbx.proxy.parser.ast.stmt.reload.ReloadUsers;
import com.alibaba.polardbx.proxy.parser.ast.stmt.tms.TMSOptimizeTableStatement;
import com.alibaba.polardbx.proxy.parser.util.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_FALSE;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_NOT_FALSE;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_NOT_NULL;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_NOT_TRUE;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_NOT_UNKNOWN;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_NULL;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_TRUE;
import static com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression.IS_UNKNOWN;

/**
 * @author QIU Shuo
 */
public class MySQLOutputASTVisitor implements SQLASTVisitor {

    protected static final Object[] EMPTY_OBJ_ARRAY = new Object[0];
    protected static final int[] EMPTY_INT_ARRAY = new int[0];
    protected final StringBuilder appendable;
    protected final Object[] args;
    protected int[] argsIndex;
    protected Map<PlaceHolder, Object> placeHolderToString;
    protected boolean upperCase = false;

    public MySQLOutputASTVisitor(StringBuilder appendable) {
        this(appendable, false);
    }

    public MySQLOutputASTVisitor(StringBuilder appendable, boolean upperCase) {
        this(appendable, null, upperCase);
    }

    /**
     * @param args parameters for {@link java.sql.PreparedStatement preparedStmt}
     */
    public MySQLOutputASTVisitor(StringBuilder appendable, Object[] args, boolean upperCase) {
        this.appendable = appendable;
        this.args = args == null ? EMPTY_OBJ_ARRAY : args;
        this.argsIndex = args == null ? EMPTY_INT_ARRAY : new int[args.length];
        this.upperCase = upperCase;
    }

    public void setPlaceHolderToString(Map<PlaceHolder, Object> map) {
        this.placeHolderToString = map;
    }

    public String getSql() {
        return appendable.toString();
    }

    /**
     * @return never null. rst[i] ≡ {@link #args}[{@link #argsIndex}[i]]
     */
    public Object[] getArguments() {
        final int argsIndexSize = argsIndex.length;
        if (argsIndexSize <= 0) {
            return EMPTY_OBJ_ARRAY;
        }

        boolean noChange = true;
        for (int i = 0; i < argsIndexSize; ++i) {
            if (i != argsIndex[i]) {
                noChange = false;
                break;
            }
        }
        if (noChange) {
            return args;
        }

        Object[] rst = new Object[argsIndexSize];
        for (int i = 0; i < argsIndexSize; ++i) {
            rst[i] = args[argsIndex[i]];
        }
        return rst;
    }

    /**
     * @param list never null
     */
    protected void printList(List<? extends ASTNode> list) {
        printList(list, ", ");
    }

    /**
     * @param list never null
     */
    protected void printList(List<? extends ASTNode> list, String sep) {
        boolean isFst = true;
        for (ASTNode arg : list) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(sep);
            }
            arg.accept(this);
        }
    }

    @Override
    public void visit(BetweenAndExpression node) {
        Expression comparee = node.getFirst();
        boolean paren = comparee.getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        comparee.accept(this);
        if (paren) {
            appendable.append(')');
        }

        if (node.isNot()) {
            appendable.append(" NOT BETWEEN ");
        } else {
            appendable.append(" BETWEEN ");
        }

        Expression start = node.getSecond();
        paren = start.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        start.accept(this);
        if (paren) {
            appendable.append(')');
        }

        appendable.append(" AND ");

        Expression end = node.getThird();
        paren = end.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        end.accept(this);
        if (paren) {
            appendable.append(')');
        }
    }

    @Override
    public void visit(ComparisionIsExpression node) {
        Expression comparee = node.getOperand();
        boolean paren = comparee.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        comparee.accept(this);
        if (paren) {
            appendable.append(')');
        }
        switch (node.getMode()) {
        case IS_NULL:
            appendable.append(" IS NULL");
            break;
        case IS_TRUE:
            appendable.append(" IS TRUE");
            break;
        case IS_FALSE:
            appendable.append(" IS FALSE");
            break;
        case IS_UNKNOWN:
            appendable.append(" IS UNKNOWN");
            break;
        case IS_NOT_NULL:
            appendable.append(" IS NOT NULL");
            break;
        case IS_NOT_TRUE:
            appendable.append(" IS NOT TRUE");
            break;
        case IS_NOT_FALSE:
            appendable.append(" IS NOT FALSE");
            break;
        case IS_NOT_UNKNOWN:
            appendable.append(" IS NOT UNKNOWN");
            break;
        default:
            throw new IllegalArgumentException("unknown mode for IS expression: " + node.getMode());
        }
    }

    @Override
    public void visit(InExpressionList node) {
        appendable.append('(');
        printList(node.getList());
        appendable.append(')');
    }

    @Override
    public void visit(LikeExpression node) {
        Expression comparee = node.getFirst();
        boolean paren = comparee.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        comparee.accept(this);
        if (paren) {
            appendable.append(')');
        }

        if (node.isNot()) {
            appendable.append(" NOT LIKE ");
        } else {
            appendable.append(" LIKE ");
        }

        Expression pattern = node.getSecond();
        paren = pattern.getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        pattern.accept(this);
        if (paren) {
            appendable.append(')');
        }

        Expression escape = node.getThird();
        if (escape != null) {
            appendable.append(" ESCAPE ");
            paren = escape.getPrecedence() <= node.getPrecedence();
            if (paren) {
                appendable.append('(');
            }
            escape.accept(this);
            if (paren) {
                appendable.append(')');
            }
        }
    }

    @Override
    public void visit(CollateExpression node) {
        Expression string = node.getString();
        boolean paren = string.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        string.accept(this);
        if (paren) {
            appendable.append(')');
        }

        appendable.append(" COLLATE ").append(node.getCollateName());
    }

    @Override
    public void visit(UserExpression node) {
        appendable.append(node.getUserAtHost());
    }

    @Override
    public void visit(UnaryOperatorExpression node) {
        appendable.append(node.getOperator()).append(' ');
        boolean paren = node.getOperand().getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        node.getOperand().accept(this);
        if (paren) {
            appendable.append(')');
        }
    }

    @Override
    public void visit(BinaryOperatorExpression node) {
        Expression left = node.getLeftOprand();
        boolean paren = node.isLeftCombine() ? left.getPrecedence() < node.getPrecedence() : left
            .getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        left.accept(this);
        if (paren) {
            appendable.append(')');
        }

        appendable.append(' ').append(node.getOperator()).append(' ');

        Expression right = node.getRightOprand();
        paren = node.isLeftCombine() ? right.getPrecedence() <= node.getPrecedence() : right.getPrecedence() < node
            .getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        right.accept(this);
        if (paren) {
            appendable.append(')');
        }
    }

    @Override
    public void visit(PolyadicOperatorExpression node) {
        for (int i = 0, len = node.getArity(); i < len; ++i) {
            if (i > 0) {
                appendable.append(' ').append(node.getOperator()).append(' ');
            }
            Expression operand = node.getOperand(i);
            boolean paren = operand.getPrecedence() < node.getPrecedence();
            if (paren) {
                appendable.append('(');
            }
            operand.accept(this);
            if (paren) {
                appendable.append(')');
            }
        }
    }

    @Override
    public void visit(LogicalAndExpression node) {
        visit((PolyadicOperatorExpression) node);
    }

    @Override
    public void visit(LogicalOrExpression node) {
        visit((PolyadicOperatorExpression) node);
    }

    @Override
    public void visit(ComparisionEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(ComparisionNullSafeEqualsExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(InExpression node) {
        visit((BinaryOperatorExpression) node);
    }

    @Override
    public void visit(FunctionExpression node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Char node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        printList(node.getArguments());
        String charset = node.getCharset();
        if (charset != null) {
            appendable.append(" USING ").append(charset);
        }
        appendable.append(')');
    }

    @Override
    public void visit(Convert node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');

        printList(node.getArguments());
        String transcodeName = node.getTranscodeName();
        if (transcodeName != null) {
            appendable.append(" USING ").append(transcodeName);
        } else {
            appendable.append(", ");
            String typeName = node.getTypeName();
            appendable.append(typeName);
            Expression info1 = node.getTypeInfo1();
            if (info1 != null) {
                appendable.append('(');
                info1.accept(this);
                Expression info2 = node.getTypeInfo2();
                if (info2 != null) {
                    appendable.append(", ");
                    info2.accept(this);
                }
                appendable.append(')');
            }
        }
        appendable.append(')');
    }

    @Override
    public void visit(Trim node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        Expression remStr = node.getRemainString();
        switch (node.getDirection()) {
        case DEFAULT:
            if (remStr != null) {
                remStr.accept(this);
                appendable.append(" FROM ");
            }
            break;
        case BOTH:
            appendable.append("BOTH ");
            if (remStr != null) {
                remStr.accept(this);
            }
            appendable.append(" FROM ");
            break;
        case LEADING:
            appendable.append("LEADING ");
            if (remStr != null) {
                remStr.accept(this);
            }
            appendable.append(" FROM ");
            break;
        case TRAILING:
            appendable.append("TRAILING ");
            if (remStr != null) {
                remStr.accept(this);
            }
            appendable.append(" FROM ");
            break;
        default:
            throw new IllegalArgumentException("unknown trim direction: " + node.getDirection());
        }
        Expression str = node.getString();
        str.accept(this);
        appendable.append(')');
    }

    @Override
    public void visit(Cast node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        node.getExpr().accept(this);
        appendable.append(" AS ");
        String typeName = node.getTypeName();
        appendable.append(typeName);
        Expression info1 = node.getTypeInfo1();
        if (info1 != null) {
            appendable.append('(');
            info1.accept(this);
            Expression info2 = node.getTypeInfo2();
            if (info2 != null) {
                appendable.append(", ");
                info2.accept(this);
            }
            appendable.append(')');
        }
        appendable.append(')');
    }

    @Override
    public void visit(Avg node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Max node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Min node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Sum node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Count node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(GroupConcat node) {
        String functionName = node.getFunctionName();
        appendable.append(functionName).append('(');
        if (node.isDistinct()) {
            appendable.append("DISTINCT ");
        }
        printList(node.getArguments());
        List<Pair<Expression, SortOrder>> orderBy = node.getOrderBy();

        if (orderBy != null && !orderBy.isEmpty()) {
            // Same length guaranteed
            Iterator<Pair<Expression, SortOrder>> orderByIterator = orderBy.iterator();
            appendable.append(" ORDER BY ");
            boolean first = true;
            while (orderByIterator.hasNext()) {
                if (!first) {
                    appendable.append(",");
                }
                first = false;
                Pair<Expression, SortOrder> o = orderByIterator.next();
                o.getKey().accept(this);
                if (o.getValue().equals(SortOrder.DESC)) {
                    appendable.append(" DESC");
                } else {
                    appendable.append(" ASC");
                }
            }
        }
        LiteralString sep = node.getSeparator();
        if (sep != null) {
            appendable.append(" SEPARATOR ");
            sep.accept(this);
        }
        appendable.append(')');
    }

    @Override
    public void visit(Extract node) {
        appendable.append("EXTRACT(").append(node.getUnit().name()).append(" FROM ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Timestampdiff node) {
        appendable.append("TIMESTAMPDIFF(").append(node.getUnit().name()).append(", ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(Timestampadd node) {
        appendable.append("TIMESTAMPADD(").append(node.getUnit().name()).append(", ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(GetFormat node) {
        appendable.append("GET_FORMAT(");
        GetFormat.FormatType type = node.getFormatType();
        appendable.append(type.name()).append(", ");
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(PlaceHolder node) {
        if (placeHolderToString == null) {
            appendable.append("${").append(node.getName()).append('}');
            return;
        }
        Object toStringer = placeHolderToString.get(node);
        if (toStringer == null) {
            appendable.append("${").append(node.getName()).append('}');
        } else {
            appendable.append(toStringer.toString());
        }
    }

    @Override
    public void visit(IntervalPrimary node) {
        appendable.append("INTERVAL ");
        Expression quantity = node.getQuantity();
        boolean paren = quantity.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        quantity.accept(this);
        if (paren) {
            appendable.append(')');
        }
        IntervalPrimary.Unit unit = node.getUnit();
        appendable.append(' ').append(unit.name());
    }

    @Override
    public void visit(LiteralBitField node) {
        String introducer = node.getIntroducer();
        if (introducer != null) {
            appendable.append(introducer).append(' ');
        }
        appendable.append("b'").append(node.getText()).append('\'');
    }

    @Override
    public void visit(LiteralBoolean node) {
        if (node.isTrue()) {
            appendable.append("TRUE");
        } else {
            appendable.append("FALSE");
        }
    }

    @Override
    public void visit(LiteralHexadecimal node) {
        String introducer = node.getIntroducer();
        if (introducer != null) {
            appendable.append(introducer).append(' ');
        }
        appendable.append("x'");
        node.appendTo(appendable);
        appendable.append('\'');
    }

    @Override
    public void visit(LiteralNull node) {
        appendable.append("NULL");
    }

    @Override
    public void visit(LiteralNumber node) {
        appendable.append(String.valueOf(node.getNumber()));
    }

    @Override
    public void visit(LiteralString node) {
        String introducer = node.getIntroducer();
        if (introducer != null) {
            appendable.append(introducer);
        } else if (node.isNchars()) {
            appendable.append('N');
        }
        appendable.append('\'').append(node.getString()).append('\'');
    }

    @Override
    public void visit(CaseWhenOperatorExpression node) {
        appendable.append("CASE");
        Expression comparee = node.getComparee();
        if (comparee != null) {
            appendable.append(' ');
            comparee.accept(this);
        }
        List<Pair<Expression, Expression>> whenList = node.getWhenList();
        for (Pair<Expression, Expression> whenthen : whenList) {
            appendable.append(" WHEN ");
            Expression when = whenthen.getKey();
            when.accept(this);
            appendable.append(" THEN ");
            Expression then = whenthen.getValue();
            then.accept(this);
        }
        Expression elseRst = node.getElseResult();
        if (elseRst != null) {
            appendable.append(" ELSE ");
            elseRst.accept(this);
        }
        appendable.append(" END");
    }

    @Override
    public void visit(DefaultValue node) {
        appendable.append("DEFAULT");
    }

    @Override
    public void visit(Identifier node) {
        Expression parent = node.getParent();
        if (parent != null) {
            parent.accept(this);
            appendable.append('.');
        }

        appendable.append(convertKeywords(node.getIdText()));
    }

    protected static boolean containsCompIn(Expression pat) {
        if (pat.getPrecedence() > Expression.PRECEDENCE_COMPARISION) {
            return false;
        }
        if (pat instanceof BinaryOperatorExpression) {
            if (pat instanceof InExpression) {
                return true;
            }
            BinaryOperatorExpression bp = (BinaryOperatorExpression) pat;
            if (bp.isLeftCombine()) {
                return containsCompIn(bp.getLeftOprand());
            } else {
                return containsCompIn(bp.getLeftOprand());
            }
        } else if (pat instanceof ComparisionIsExpression) {
            ComparisionIsExpression is = (ComparisionIsExpression) pat;
            return containsCompIn(is.getOperand());
        } else if (pat instanceof TernaryOperatorExpression) {
            TernaryOperatorExpression tp = (TernaryOperatorExpression) pat;
            return containsCompIn(tp.getFirst()) || containsCompIn(tp.getSecond()) || containsCompIn(tp.getThird());
        } else if (pat instanceof UnaryOperatorExpression) {
            UnaryOperatorExpression up = (UnaryOperatorExpression) pat;
            return containsCompIn(up.getOperand());
        } else {
            return false;
        }
    }

    @Override
    public void visit(MatchExpression node) {
        appendable.append("MATCH (");
        printList(node.getColumns());
        appendable.append(") AGAINST (");
        Expression pattern = node.getPattern();
        boolean inparen = containsCompIn(pattern);
        if (inparen) {
            appendable.append('(');
        }
        pattern.accept(this);
        if (inparen) {
            appendable.append(')');
        }
        switch (node.getModifier()) {
        case IN_BOOLEAN_MODE:
            appendable.append(" IN BOOLEAN MODE");
            break;
        case IN_NATURAL_LANGUAGE_MODE:
            appendable.append(" IN NATURAL LANGUAGE MODE");
            break;
        case IN_NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION:
            appendable.append(" IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION");
            break;
        case WITH_QUERY_EXPANSION:
            appendable.append(" WITH QUERY EXPANSION");
            break;
        case _DEFAULT:
            break;
        default:
            throw new IllegalArgumentException("unkown modifier for match expression: " + node.getModifier());
        }
        appendable.append(')');
    }

    protected int index = -1;

    protected void appendArgsIndex(int value) {
        int i = ++index;
        if (argsIndex.length <= i) {
            int[] a = new int[i + 1];
            if (i > 0) {
                System.arraycopy(argsIndex, 0, a, 0, i);
            }
            argsIndex = a;
        }
        argsIndex[i] = value;
    }

    @Override
    public void visit(ParamMarker node) {
        appendable.append('?');
        appendArgsIndex(node.getParamIndex() - 1);
    }

    @Override
    public void visit(RowExpression node) {
        appendable.append("ROW(");
        printList(node.getRowExprList());
        appendable.append(')');
    }

    @Override
    public void visit(SysVarPrimary node) {
        VariableScope scope = node.getScope();
        switch (scope) {
        case GLOBAL:
            appendable.append("@@global.");
            break;
        case SESSION:
            appendable.append("@@");
            break;
        default:
            throw new IllegalArgumentException("unkown scope for sysVar primary: " + scope);
        }
        appendable.append(node.getVarText());
    }

    @Override
    public void visit(UsrDefVarPrimary node) {
        appendable.append(node.getVarText());
    }

    @Override
    public void visit(IndexHint node) {
        IndexHint.IndexAction action = node.getAction();
        switch (action) {
        case FORCE:
            appendable.append("FORCE ");
            break;
        case IGNORE:
            appendable.append("IGNORE ");
            break;
        case USE:
            appendable.append("USE ");
            break;
        default:
            throw new IllegalArgumentException("unkown index action for index hint: " + action);
        }
        IndexHint.IndexType type = node.getType();
        switch (type) {
        case INDEX:
            appendable.append("INDEX ");
            break;
        case KEY:
            appendable.append("KEY ");
            break;
        default:
            throw new IllegalArgumentException("unkown index type for index hint: " + type);
        }
        IndexHint.IndexScope scope = node.getScope();
        switch (scope) {
        case GROUP_BY:
            appendable.append("FOR GROUP BY ");
            break;
        case ORDER_BY:
            appendable.append("FOR ORDER BY ");
            break;
        case JOIN:
            appendable.append("FOR JOIN ");
            break;
        case ALL:
            break;
        default:
            throw new IllegalArgumentException("unkown index scope for index hint: " + scope);
        }
        appendable.append('(');
        List<String> indexList = node.getIndexList();
        boolean isFst = true;
        for (String indexName : indexList) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            appendable.append(convertKeywords(indexName));
        }
        appendable.append(')');
    }

    @Override
    public void visit(TableReferences node) {
        printList(node.getTableReferenceList());
    }

    @Override
    public void visit(InnerJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        left.accept(this);
        if (paren) {
            appendable.append(')');
        }

        appendable.append(" INNER JOIN ");
        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        right.accept(this);
        if (paren) {
            appendable.append(')');
        }

        Expression on = node.getOnCond();
        List<String> using = node.getUsing();
        if (on != null) {
            appendable.append(" ON ");
            on.accept(this);
        } else if (using != null) {
            appendable.append(" USING (");
            boolean isFst = true;
            for (String col : using) {
                if (isFst) {
                    isFst = false;
                } else {
                    appendable.append(", ");
                }
                appendable.append(convertKeywords(col));
            }
            appendable.append(")");
        }
    }

    @Override
    public void visit(NaturalJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        left.accept(this);
        if (paren) {
            appendable.append(')');
        }

        appendable.append(" NATURAL ");
        if (node.isOuter()) {
            if (node.isLeft()) {
                appendable.append("LEFT ");
            } else {
                appendable.append("RIGHT ");
            }
        }
        appendable.append("JOIN ");

        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        right.accept(this);
        if (paren) {
            appendable.append(')');
        }
    }

    @Override
    public void visit(StraightJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        left.accept(this);
        if (paren) {
            appendable.append(')');
        }

        appendable.append(" STRAIGHT_JOIN ");

        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        right.accept(this);
        if (paren) {
            appendable.append(')');
        }

        Expression on = node.getOnCond();
        if (on != null) {
            appendable.append(" ON ");
            on.accept(this);
        }
    }

    @Override
    public void visit(OuterJoin node) {
        TableReference left = node.getLeftTableRef();
        boolean paren = left.getPrecedence() < node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        left.accept(this);
        if (paren) {
            appendable.append(')');
        }

        if (node.isLeftJoin()) {
            appendable.append(" LEFT JOIN ");
        } else {
            appendable.append(" RIGHT JOIN ");
        }

        TableReference right = node.getRightTableRef();
        paren = right.getPrecedence() <= node.getPrecedence();
        if (paren) {
            appendable.append('(');
        }
        right.accept(this);
        if (paren) {
            appendable.append(')');
        }

        Expression on = node.getOnCond();
        List<String> using = node.getUsing();
        if (on != null) {
            appendable.append(" ON ");
            on.accept(this);
        } else if (using != null) {
            appendable.append(" USING (");
            boolean isFst = true;
            for (String col : using) {
                if (isFst) {
                    isFst = false;
                } else {
                    appendable.append(", ");
                }
                appendable.append(convertKeywords(col));
            }
            appendable.append(")");
        } else {
            throw new IllegalArgumentException("either ON or USING must be included for OUTER JOIN");
        }
    }

    @Override
    public void visit(SubqueryFactor node) {
        appendable.append('(');
        QueryExpression query = node.getSubquery();
        query.accept(this);
        appendable.append(") AS ").append(convertKeywords(node.getAlias()));
    }

    @Override
    public void visit(TableRefFactor node) {
        processTableName(node.getTable());
        String alias = node.getAlias();
        if (alias != null) {
            appendable.append(" AS ");
            appendable.append(convertKeywords(alias));
        }
        List<IndexHint> list = node.getHintList();
        if (list != null && !list.isEmpty()) {
            appendable.append(' ');
            printList(list, " ");
        }
    }

    @Override
    public void visit(Dual dual) {
        appendable.append("DUAL");
    }

    @Override
    public void visit(GroupBy node) {
        appendable.append("GROUP BY ");
        boolean isFst = true;
        for (Pair<Expression, SortOrder> p : node.getOrderByList()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            Expression col = p.getKey();
            col.accept(this);
            switch (p.getValue()) {
            case DESC:
                appendable.append(" DESC");
                break;
            default:
                break;
            }
        }
        if (node.isWithRollup()) {
            appendable.append(" WITH ROLLUP");
        }
    }

    @Override
    public void visit(OrderBy node) {
        appendable.append("ORDER BY ");
        boolean isFst = true;
        for (Pair<Expression, SortOrder> p : node.getOrderByList()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            Expression col = p.getKey();

            if (col instanceof DMLSelectStatement) {
                appendable.append("( ");
                col.accept(this);
                appendable.append(" )");
            } else {
                col.accept(this);
            }

            switch (p.getValue()) {
            case DESC:
                appendable.append(" DESC");
                break;
            default:
                break;
            }
        }
    }

    @Override
    public void visit(Limit node) {
        appendable.append("LIMIT ");

        boolean isPostpositiveOffset = node.isPostpositiveOffset();
        Object offset = node.getOffset();
        Object size = node.getSize();

        if (!isPostpositiveOffset) {

            /**
             * 用户输入的的LIMIT语句是：
             *
             * <pre>
             *  LIMIT xxx, xxx 语法 或 LIMIT ?, ?
             * </pre>
             */
            if (offset instanceof ParamMarker) {
                ((ParamMarker) offset).accept(this);
            } else {
                appendable.append(String.valueOf(offset));
            }
            appendable.append(", ");

            if (size instanceof ParamMarker) {
                ((ParamMarker) size).accept(this);
            } else {
                appendable.append(String.valueOf(size));
            }
        } else {

            /**
             * 用户输入的LIMIT语句是：
             *
             * <pre>
             * (1) LIMIT xxx 语法 或 LIMIT ?;
             * (2) LIMIT xxx OFFSET xxx 或  LIMIT ? OFFSET ?
             * </pre>
             */
            if (size instanceof ParamMarker) {
                ((ParamMarker) size).accept(this);
            } else {
                appendable.append(String.valueOf(size));
            }
            appendable.append(" OFFSET ");
            if (offset instanceof ParamMarker) {
                ((ParamMarker) offset).accept(this);
            } else {
                appendable.append(String.valueOf(offset));
            }

        }
    }

    @Override
    public void visit(IndexOption node) {
        if (node.getKeyBlockSize() != null) {
            appendable.append("KEY_BLOCK_SIZE = ");
            node.getKeyBlockSize().accept(this);
        } else if (node.getIndexType() != null) {
            appendable.append("USING ");
            switch (node.getIndexType()) {// USING {BTREE | HASH}
            case BTREE:
                appendable.append("BTREE");
                break;
            case HASH:
                appendable.append("HASH");
                break;
            }
        } else if (node.getParserName() != null) {
            appendable.append("WITH PARSER ");
            node.getParserName().accept(this);
            appendable.append(" ");
        } else if (node.getComment() != null) {
            appendable.append("COMMENT ");
            node.getComment().accept(this);
            appendable.append(" ");
        }
    }

    private static class CommerSpliter {
        private StringBuilder appendable;
        private boolean first = true;

        public CommerSpliter(StringBuilder appendable) {
            this.appendable = appendable;
        }

        public void split() {
            if (first) {
                first = false;
            } else {
                appendable.append(", ");
            }
        }

        public void reset() {
            first = true;
        }
    }

    @Override
    public void visit(TableOptions node) {
        /* table_option [[,] table_option] ... */
        CommerSpliter spliter = new CommerSpliter(appendable);
        spliter.reset();
        /* ENGINE [=] engine_name */
        if (node.getEngine() != null) {
            spliter.split();
            appendable.append("ENGINE = ");
            node.getEngine().accept(this);
        }
        /* AUTO_INCREMENT [=] value */
        if (node.getAutoIncrement() != null) {
            spliter.split();
            appendable.append("AUTO_INCREMENT = ");
            ((LiteralNumber) node.getAutoIncrement()).accept(this);
        }
        /* AVG_ROW_LENGTH [=] value */
        if (node.getAvgRowLength() != null) {
            spliter.split();
            appendable.append("AVG_ROW_LENGTH = ");
            ((LiteralNumber) node.getAvgRowLength()).accept(this);
        }

        /* [DEFAULT] CHARACTER SET [=] charset_name */
        if (node.getCharSet() != null) {
            spliter.split();
            if (node.isDefaultCharset()) {
                appendable.append("DEFAULT ");
            }
            appendable.append("CHARACTER SET = ");
            node.getCharSet().accept(this);
            /**
             * 因为mysql实际支持tableOptions中一个表达式中同时存在CHARACTER SET和COLLATE的情况，
             * 这里为了与单独的COLLATE区别，用了collateWithCharaset
             */
            if (node.getCollateWithCharset() != null) {
                if (node.isDefaultCollateWithCharset()) {
                    appendable.append(" DEFAULT");
                }
                appendable.append(" COLLATE = ");
                node.getCollateWithCharset().accept(this);
            }
        }
        /* CHECKSUM [=] {0 | 1} */
        if (node.getCheckSum() != null) {
            spliter.split();
            appendable.append("CHECKSUM = ");
            appendable.append(node.getCheckSum() ? 1 : 0);
        }
        /* [DEFAULT] COLLATE [=] collation_name */
        if (node.getCollation() != null) {
            spliter.split();
            if (node.isDefaultCollate()) {
                appendable.append("DEFAULT ");
            }
            appendable.append("COLLATE = ");
            node.getCollation().accept(this);
        }
        /* COMMENT [=] 'string' */
        if (node.getComment() != null) {
            spliter.split();
            appendable.append("COMMENT = ");
            node.getComment().accept(this);
        }
        /* CONNECTION [=] 'connect_string' */
        if (node.getConnection() != null) {
            spliter.split();
            appendable.append("CONNECTION = ");
            node.getConnection().accept(this);
        }
        /* DATA DIRECTORY [=] 'absolute path to directory' */
        if (node.getDataDir() != null) {
            spliter.split();
            appendable.append("DATA DIRECTORY = ");
            node.getDataDir().accept(this);
        }
        /* DELAY_KEY_WRITE [=] {0 | 1} */
        if (node.getDelayKeyWrite() != null) {
            spliter.split();
            appendable.append("DELAY_KEY_WRITE = ");
            appendable.append(node.getDelayKeyWrite() ? 1 : 0);
        }

        /* INDEX DIRECTORY [=] 'absolute path to directory' */
        if (node.getIndexDir() != null) {
            spliter.split();
            appendable.append("INDEX DIRECTORY = ");
            node.getIndexDir().accept(this);
        }
        /* INSERT_METHOD [=] { NO | FIRST | LAST } */
        if (node.getInsertMethod() != null) {
            spliter.split();
            appendable.append("INSERT_METHOD = ");
            appendable.append(node.getInsertMethod().name());
        }
        /* KEY_BLOCK_SIZE [=] value */
        if (node.getKeyBlockSize() != null) {
            spliter.split();
            appendable.append("KEY_BLOCK_SIZE = ");
            ((LiteralNumber) node.getKeyBlockSize()).accept(this);
        }
        /* MAX_ROWS [=] value */
        if (node.getMaxRows() != null) {
            spliter.split();
            appendable.append("MAX_ROWS = ");
            ((LiteralNumber) node.getMaxRows()).accept(this);
        }
        /* MIN_ROWS [=] value */
        if (node.getMinRows() != null) {
            spliter.split();
            appendable.append("MIN_ROWS = ");
            ((LiteralNumber) node.getMinRows()).accept(this);
        }
        /* PACK_KEYS [=] {0 | 1 | DEFAULT} */
        if (node.getPackKeys() != null) {
            spliter.split();
            appendable.append("PACK_KEYS = ");
            switch (node.getPackKeys()) {
            case TRUE:
                appendable.append(1);
                break;
            case FALSE:
                appendable.append(0);
                break;
            case DEFAULT:
                appendable.append("DEFAULT");
                break;
            }
        }

        /* PASSWORD [=] 'string' */
        if (node.getPassword() != null) {
            spliter.split();
            appendable.append("PASSWORD = ");
            node.getPassword().accept(this);
        }
        /*
         * ROW_FORMAT [=] {DEFAULT|DYNAMIC|FIXED|COMPRESSED|REDUNDANT|COMPACT}
         */
        if (node.getRowFormat() != null) {
            spliter.split();
            appendable.append("ROW_FORMAT = ");
            appendable.append(node.getRowFormat().name());
        }

        /* STATS_AUTO_RECALC [=] {DEFAULT|0|1} */
        if (node.getStatsAutoRecalc() != null) {
            spliter.split();
            appendable.append("STATS_AUTO_RECALC = ");
            switch (node.getStatsAutoRecalc()) {
            case FALSE:
                appendable.append(0);
                break;
            case TRUE:
                appendable.append(1);
                break;
            case DEFAULT:
                appendable.append("DEFAULT");
                break;
            }
        }

        /* STATS_PERSISTENT [=] {DEFAULT|0|1} */
        if (node.getStatsPersistent() != null) {
            spliter.split();
            appendable.append("STATS_PERSISTENT = ");
            switch (node.getStatsPersistent()) {
            case FALSE:
                appendable.append(0);
                break;
            case TRUE:
                appendable.append(1);
                break;
            case DEFAULT:
                appendable.append("DEFAULT");
                break;
            }
        }

        /* STATS_SAMPLE_PAGES [=] value */
        if (node.getStatsSamplePages() != null) {
            spliter.split();
            appendable.append("STATS_SAMPLE_PAGES = ");
            node.getStatsSamplePages().accept(this);
        }

        /* TABLESPACE tablespace_name [STORAGE {DISK|MEMORY|DEFAULT}] */
        if (node.getTablespaceName() != null) {
            spliter.split();
            ;

            appendable.append("TABLESPACE ");
            node.getTablespaceName().accept(this);
            if (node.getTableSpaceStorage() != null) {
                appendable.append(" STORAGE ");
                switch (node.getTableSpaceStorage()) {
                case DISK:
                    appendable.append("DISK");
                    break;
                case MEMORY:
                    appendable.append("MEMORY");
                    break;
                case DEFAULT:
                    appendable.append("DEFAULT");
                    break;
                }
            }
        }

        /* UNION [=] (tbl_name[,tbl_name]...) */
        if (node.getUnion() != null) {
            spliter.split();
            appendable.append("UNION = (");
            CommerSpliter unspliter = new CommerSpliter(appendable);
            for (Identifier tbl_name : node.getUnion()) {
                unspliter.split();
                tbl_name.accept(this);
            }
            appendable.append(")");
        }
    }

    @Override
    public void visit(DataType node) {
        throw new UnsupportedOperationException("subclass have not implement visit");
    }

    @Override
    public void visit(ShowDbLock showDbLock) {
        appendable.append("SHOW ").append("DBLOCK");
    }

    protected void printSimpleShowStmt(String attName) {
        appendable.append("SHOW ").append(attName);
    }

    @Override
    public void visit(ShowAuthors node) {
        printSimpleShowStmt("AUTHORS");
    }

    @Override
    public void visit(ShowBinaryLog node) {
        printSimpleShowStmt("BINARY LOGS");
    }

    @Override
    public void visit(ShowBinLogEvent node) {
        appendable.append("SHOW BINLOG EVENTS");
        String logName = node.getLogName();
        if (logName != null) {
            appendable.append(" IN ").append(logName);
        }
        Expression pos = node.getPos();
        if (pos != null) {
            appendable.append(" FROM ");
            pos.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    /**
     * ' ' will be prepended
     */
    protected void printLikeOrWhere(String like, Expression where) {
        if (like != null) {
            appendable.append(" LIKE ").append(like);
        } else if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
    }

    @Override
    public void visit(ShowCharaterSet node) {
        appendable.append("SHOW CHARACTER SET");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowCollation node) {
        appendable.append("SHOW COLLATION");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowColumns node) {
        appendable.append("SHOW ");
        if (node.isFull()) {
            appendable.append("FULL ");
        }
        appendable.append("COLUMNS FROM ");
        processTableName(node.getTable());
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowContributors node) {
        printSimpleShowStmt("CONTRIBUTORS");
    }

    @Override
    public void visit(ShowCreate node) {
        appendable.append("SHOW CREATE ").append(node.getType().name()).append(' ');
        processTableName(node.getId());
    }

    @Override
    public void visit(ShowDatabases node) {
        appendable.append("SHOW DATABASES");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowEngine node) {
        appendable.append("SHOW ENGINE ");
        switch (node.getType()) {
        case INNODB_MUTEX:
            appendable.append("INNODB MUTEX");
            break;
        case INNODB_STATUS:
            appendable.append("INNODB STATUS");
            break;
        case PERFORMANCE_SCHEMA_STATUS:
            appendable.append("PERFORMANCE SCHEMA STATUS");
            break;
        default:
            throw new IllegalArgumentException("unrecognized type for SHOW ENGINE: " + node.getType());
        }
    }

    @Override
    public void visit(ShowEngines node) {
        printSimpleShowStmt("ENGINES");
    }

    @Override
    public void visit(ShowErrors node) {
        appendable.append("SHOW ");
        if (node.isCount()) {
            appendable.append("COUNT(*) ERRORS");
        } else {
            appendable.append("ERRORS");
            Limit limit = node.getLimit();
            if (node.getLimit() != null) {
                appendable.append(' ');
                limit.accept(this);
            }
        }
    }

    @Override
    public void visit(ShowEvents node) {
        appendable.append("SHOW EVENTS");
        Identifier schema = node.getSchema();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowFunctionCode node) {
        appendable.append("SHOW FUNCTION CODE ");
        node.getFunctionName().accept(this);
    }

    @Override
    public void visit(ShowFunctionStatus node) {
        appendable.append("SHOW FUNCTION STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowGrantsStatement node) {
        if (node.isForAll()) {
            appendable.append("SHOW GRANTS");
        } else {
            appendable.append("SHOW GRANTS FOR ");
            appendable.append("'").append(node.getUser()).append("'");
            appendable.append("@");
            appendable.append("'").append(node.getHost()).append("'");
        }
    }

    @Override
    public void visit(ShowIndex node) {
        appendable.append("SHOW ");
        switch (node.getType()) {
        case INDEX:
            appendable.append("INDEX ");
            break;
        case INDEXES:
            appendable.append("INDEXES ");
            break;
        case KEYS:
            appendable.append("KEYS ");
            break;
        default:
            throw new IllegalArgumentException("unrecognized type for SHOW INDEX: " + node.getType());
        }
        appendable.append("IN ");
        processTableName(node.getTable());
    }

    @Override
    public void visit(ShowMasterStatus node) {
        printSimpleShowStmt("MASTER STATUS");
    }

    @Override
    public void visit(ShowOpenTables node) {
        appendable.append("SHOW OPEN TABLES");
        Identifier db = node.getSchema();
        if (db != null) {
            appendable.append(" FROM ");
            db.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowPlugins node) {
        printSimpleShowStmt("PLUGINS");
    }

    @Override
    public void visit(ShowPrivileges node) {
        printSimpleShowStmt("PRIVILEGES");
    }

    @Override
    public void visit(ShowProcedureCode node) {
        appendable.append("SHOW PROCEDURE CODE ");
        node.getProcedureName().accept(this);
    }

    @Override
    public void visit(ShowProcedureStatus node) {
        appendable.append("SHOW PROCEDURE STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowProcesslist node) {
        appendable.append("SHOW ");
        if (node.isFull()) {
            appendable.append("FULL ");
        }
        appendable.append("PROCESSLIST");
    }

    @Override
    public void visit(ShowProfile node) {
        appendable.append("SHOW PROFILE");
        List<ShowProfile.Type> types = node.getTypes();
        boolean isFst = true;
        for (ShowProfile.Type type : types) {
            if (isFst) {
                isFst = false;
                appendable.append(' ');
            } else {
                appendable.append(", ");
            }
            appendable.append(type.name().replace('_', ' '));
        }
        Expression query = node.getForQuery();
        if (query != null) {
            appendable.append(" FOR QUERY ");
            query.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    @Override
    public void visit(ShowProfiles node) {
        printSimpleShowStmt("PROFILES");
    }

    @Override
    public void visit(ShowSlaveHosts node) {
        printSimpleShowStmt("SLAVE HOSTS");
    }

    @Override
    public void visit(ShowSlaveStatus node) {
        printSimpleShowStmt("SLAVE STATUS");
    }

    @Override
    public void visit(ShowStatus node) {
        appendable.append("SHOW ").append(node.getScope().name().replace('_', ' ')).append(" STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowStc node) {
        appendable.append("SHOW ").append(" STC");
    }

    @Override
    public void visit(ShowTables node) {
        appendable.append("SHOW");
        if (node.isFull()) {
            appendable.append(" FULL");
        }
        appendable.append(" TABLES");
        Identifier schema = node.getSchema();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowTableStatus node) {
        appendable.append("SHOW TABLE STATUS");
        Identifier schema = node.getDatabase();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowTriggers node) {
        appendable.append("SHOW TRIGGERS");
        Identifier schema = node.getSchema();
        if (schema != null) {
            appendable.append(" FROM ");
            schema.accept(this);
        }
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowVariables node) {
        appendable.append("SHOW ").append(node.getScope().name().replace('_', ' ')).append(" VARIABLES");
        printLikeOrWhere(node.getPattern(), node.getWhere());
    }

    @Override
    public void visit(ShowWarnings node) {
        appendable.append("SHOW ");
        if (node.isCount()) {
            appendable.append("COUNT(*) WARNINGS");
        } else {
            appendable.append("WARNINGS");
            Limit limit = node.getLimit();
            if (limit != null) {
                appendable.append(' ');
                limit.accept(this);
            }
        }
    }

    @Override
    public void visit(DescTableStatement node) {
        appendable.append("DESC ");
        processTableName(node.getTable());
    }

    @Override
    public void visit(DALSetStatement node) {
        appendable.append("SET ");
        boolean isFst = true;
        for (Pair<VariableExpression, Expression> p : node.getAssignmentList()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            p.getKey().accept(this);
            appendable.append(" = ");
            p.getValue().accept(this);
        }
    }

    @Override
    public void visit(DALSetSimpleStatement node) {
        appendable.append("SET ");
        boolean isFst = true;
        for (Pair<String, String> p : node.getVals()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            appendable.append(p.getKey());
            appendable.append(" = ");
            appendable.append(p.getValue());
        }
    }

    @Override
    public void visit(DALSetNamesStatement node) {
        appendable.append("SET NAMES ");
        if (node.isDefault()) {
            appendable.append("DEFAULT");
        } else {
            appendable.append(node.getCharsetName());
            String collate = node.getCollationName();
            if (collate != null) {
                appendable.append(" COLLATE ");
                appendable.append(collate);
            }
        }
    }

    @Override
    public void visit(DALSetCharacterSetStatement node) {
        appendable.append("SET CHARACTER SET ");
        if (node.isDefault()) {
            appendable.append("DEFAULT");
        } else {
            appendable.append(node.getCharset());
        }
    }

    @Override
    public void visit(MTSSetTransactionStatement node) {
        appendable.append("SET ");
        VariableScope scope = node.getScope();
        if (scope != null) {
            switch (scope) {
            case SESSION:
                appendable.append("SESSION ");
                break;
            case GLOBAL:
                appendable.append("GLOBAL ");
                break;
            default:
                throw new IllegalArgumentException("unknown scope for SET TRANSACTION ISOLATION LEVEL: " + scope);
            }
        }
        appendable.append("TRANSACTION ISOLATION LEVEL ");
        switch (node.getLevel()) {
        case READ_COMMITTED:
            appendable.append("READ COMMITTED");
            break;
        case READ_UNCOMMITTED:
            appendable.append("READ UNCOMMITTED");
            break;
        case REPEATABLE_READ:
            appendable.append("REPEATABLE READ");
            break;
        case SERIALIZABLE:
            appendable.append("SERIALIZABLE");
            break;
        default:
            throw new IllegalArgumentException(
                "unknown level for SET TRANSACTION ISOLATION LEVEL: " + node.getLevel());
        }
    }

    @Override
    public void visit(MTSSavepointStatement node) {
        appendable.append("SAVEPOINT ");
        node.getSavepoint().accept(this);
    }

    @Override
    public void visit(MTSReleaseStatement node) {
        appendable.append("RELEASE SAVEPOINT ");
        node.getSavepoint().accept(this);
    }

    @Override
    public void visit(MTSRollbackStatement node) {
        appendable.append("ROLLBACK");
        Identifier savepoint = node.getSavepoint();
        if (savepoint == null) {
            MTSRollbackStatement.CompleteType type = node.getCompleteType();
            switch (type) {
            case CHAIN:
                appendable.append(" AND CHAIN");
                break;
            case NO_CHAIN:
                appendable.append(" AND NO CHAIN");
                break;
            case NO_RELEASE:
                appendable.append(" NO RELEASE");
                break;
            case RELEASE:
                appendable.append(" RELEASE");
                break;
            case UN_DEF:
                break;
            default:
                throw new IllegalArgumentException("unrecgnized complete type: " + type);
            }
        } else {
            appendable.append(" TO SAVEPOINT ");
            savepoint.accept(this);
        }
    }

    @Override
    public void visit(DMLCallStatement node) {
        appendable.append("CALL ");
        node.getProcedure().accept(this);
        appendable.append('(');
        printList(node.getArguments());
        appendable.append(')');
    }

    @Override
    public void visit(DMLDeleteStatement node) {
        appendable.append("DELETE ");
        if (node.isLowPriority()) {
            appendable.append("LOW_PRIORITY ");
        }
        if (node.isQuick()) {
            appendable.append("QUICK ");
        }
        if (node.isIgnore()) {
            appendable.append("IGNORE ");
        }
        TableReferences tableRefs = node.getTableRefs();
        if (tableRefs == null) {
            appendable.append("FROM ");
            processTableName(node.getTableNames().get(0));
        } else {
            printList(node.getTableNames());
            appendable.append(" FROM ");
            node.getTableRefs().accept(this);
        }
        Expression where = node.getWhereCondition();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
        OrderBy orderBy = node.getOrderBy();
        if (orderBy != null) {
            appendable.append(' ');
            orderBy.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(" LIMIT ");
            Object size = limit.getSize();
            if (size instanceof ParamMarker) {
                ((ParamMarker) size).accept(this);
            } else {
                appendable.append(String.valueOf(size));
            }
        }
    }

    @Override
    public void visit(DMLInsertStatement node) {
        appendable.append("INSERT ");
        switch (node.getMode()) {
        case DELAY:
            appendable.append("DELAYED ");
            break;
        case HIGH:
            appendable.append("HIGH_PRIORITY ");
            break;
        case LOW:
            appendable.append("LOW_PRIORITY ");
            break;
        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }

        if (node.isCommitOnSuccess()) {
            appendable.append("COMMIT_ON_SUCCESS ");
        }

        if (node.isRollbackOnFail()) {
            appendable.append("ROLLBACK_ON_FAIL ");
        }

        if (node.isQueueOnPk()) {
            appendable.append("QUEUE_ON_PK ");

            if (node.getQueueOnPkNumP() != null) {
                node.getQueueOnPkNumP().accept(this);
            } else {
                appendable.append(String.valueOf(node.getQueueOnPkNum()));
            }

            appendable.append(" ");

        }

        if (node.isTargetAffectRow()) {
            appendable.append("TARGET_AFFECT_ROW ");

            if (node.getNumP() != null) {
                node.getNumP().accept(this);
            } else {
                appendable.append(String.valueOf(node.getNum()));
            }

            appendable.append(" ");

        }

        if (node.isIgnore()) {
            appendable.append("IGNORE ");
        }

        appendable.append("INTO ");
        processTableName(node.getTable());
        appendable.append(' ');

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        QueryExpression select = node.getSelect();
        if (select == null) {
            appendable.append("VALUES ");
            List<RowExpression> rows = node.getRowList();
            if (rows != null && !rows.isEmpty()) {
                boolean isFst = true;
                for (RowExpression row : rows) {
                    if (row == null || row.getRowExprList().isEmpty()) {
                        continue;
                    }
                    if (isFst) {
                        isFst = false;
                    } else {
                        appendable.append(", ");
                    }
                    appendable.append('(');
                    printList(row.getRowExprList());
                    appendable.append(')');
                }
            } else {
                throw new IllegalArgumentException("at least one row for INSERT");
            }
        } else {
            select.accept(this);
        }

        List<Pair<Identifier, Expression>> dup = node.getDuplicateUpdate();
        if (dup != null && !dup.isEmpty()) {
            appendable.append(" ON DUPLICATE KEY UPDATE ");
            boolean isFst = true;
            for (Pair<Identifier, Expression> p : dup) {
                if (isFst) {
                    isFst = false;
                } else {
                    appendable.append(", ");
                }
                p.getKey().accept(this);
                appendable.append(" = ");
                p.getValue().accept(this);
            }
        }
    }

    @Override
    public void visit(DMLReplaceStatement node) {
        appendable.append("REPLACE ");
        switch (node.getMode()) {
        case DELAY:
            appendable.append("DELAYED ");
            break;
        case LOW:
            appendable.append("LOW_PRIORITY ");
            break;
        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }
        appendable.append("INTO ");
        processTableName(node.getTable());
        appendable.append(' ');

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        QueryExpression select = node.getSelect();
        if (select == null) {
            appendable.append("VALUES ");
            List<RowExpression> rows = node.getRowList();
            if (rows != null && !rows.isEmpty()) {
                boolean isFst = true;
                for (RowExpression row : rows) {
                    if (row == null || row.getRowExprList().isEmpty()) {
                        continue;
                    }
                    if (isFst) {
                        isFst = false;
                    } else {
                        appendable.append(", ");
                    }
                    appendable.append('(');
                    printList(row.getRowExprList());
                    appendable.append(')');
                }
            } else {
                throw new IllegalArgumentException("at least one row for REPLACE");
            }
        } else {
            select.accept(this);
        }
    }

    @Override
    public void visit(DMLSelectFromUpdateStatement node) {
        if (node.isExplain()) {
            appendable.append("EXPLAIN ");
        }

        appendable.append("SELECT ");
        final SelectFromUpdateOption option = node.getSelectFromUpdateOption();

        boolean isFst = true;
        List<Pair<Expression, String>> exprList = node.getSelectExprList();

        for (Pair<Expression, String> p : exprList) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            if (p.getKey() instanceof DMLSelectStatement) {
                appendable.append("(");
            }

            if (!upperCase) {
                appendable.append(p.getKey().getOriginStr());
            } else {
                p.getKey().accept(this);
            }

            if (p.getKey() instanceof DMLSelectStatement) {
                appendable.append(")");
            }
            String alias = p.getValue();
            if (alias != null) {
                appendable.append(" AS ").append(convertKeywords(alias));
            }
        }

        appendable.append(" FROM UPDATE ");

        if (option.lowPriority) {
            appendable.append("LOW_PRIORITY ");
        }
        if (option.ignore) {
            appendable.append("IGNORE ");
        }

        if (option.commitOnSuccess) {
            appendable.append("COMMIT_ON_SUCCESS ");
        }

        if (option.rollbackOnFail) {
            appendable.append("ROLLBACK_ON_FAIL ");
        }

        if (option.queueOnPk) {
            appendable.append("QUEUE_ON_PK ");
            if (option.queueOnPkNumP != null) {
                option.queueOnPkNumP.accept(this);
            } else {
                appendable.append(String.valueOf(option.queueOnPkNum));
            }

            appendable.append(" ");
        }

        if (option.targetAffectRow) {
            appendable.append("TARGET_AFFECT_ROW ");

            if (option.numP != null) {
                option.numP.accept(this);
            } else {
                appendable.append(String.valueOf(option.num));
            }

            appendable.append(" ");

        }

        processTableName(node.getTable());

        appendable.append(" SET ");
        isFst = true;
        for (Pair<Identifier, Expression> p : node.getValues()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            p.getKey().accept(this);
            appendable.append(" = ");
            Expression value = p.getValue();
            boolean paren = value.getPrecedence() <= Expression.PRECEDENCE_COMPARISION;
            if (paren) {
                appendable.append('(');
            }
            value.accept(this);
            if (paren) {
                appendable.append(')');
            }
        }
        Expression where = node.getWhere();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
        OrderBy order = node.getOrder();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(" LIMIT ");
            Object size = limit.getSize();
            if (size instanceof ParamMarker) {
                ((ParamMarker) size).accept(this);
            } else {
                appendable.append(String.valueOf(size));
            }
        }

    }

    @Override
    public void visit(DMLSelectStatement node) {

        if (node.isExplain()) {
            appendable.append("EXPLAIN ");
        }
        appendable.append("SELECT ");
        final DMLSelectStatement.SelectOption option = node.getOption();
        switch (option.resultDup) {
        case ALL:
            break;
        case DISTINCT:
            appendable.append("DISTINCT ");
            break;
        case DISTINCTROW:
            appendable.append("DISTINCTROW ");
            break;
        default:
            throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
        if (option.highPriority) {
            appendable.append("HIGH_PRIORITY ");
        }
        if (option.straightJoin) {
            appendable.append("STRAIGHT_JOIN ");
        }
        switch (option.resultSize) {
        case SQL_BIG_RESULT:
            appendable.append("SQL_BIG_RESULT ");
            break;
        case SQL_SMALL_RESULT:
            appendable.append("SQL_SMALL_RESULT ");
            break;
        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
        if (option.sqlBufferResult) {
            appendable.append("SQL_BUFFER_RESULT ");
        }
        switch (option.queryCache) {
        case SQL_CACHE:
            appendable.append("SQL_CACHE ");
            break;
        case SQL_NO_CACHE:
            appendable.append("SQL_NO_CACHE ");
            break;
        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
        if (option.sqlCalcFoundRows) {
            appendable.append("SQL_CALC_FOUND_ROWS ");
        }

        boolean isFst = true;
        List<Pair<Expression, String>> exprList = node.getSelectExprList();

        for (Pair<Expression, String> p : exprList) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            if (p.getKey() instanceof DMLSelectStatement) {
                appendable.append("(");
            }

            if (!upperCase) {
                if (p.getKey() instanceof DMLSelectStatement && p.getValue() != null) {
                    // 处理嵌套select
                    p.getKey().accept(this);
                } else {
                    appendable.append(p.getKey().getOriginStr());
                }
            } else {
                p.getKey().accept(this);
            }

            if (p.getKey() instanceof DMLSelectStatement) {
                appendable.append(")");
            }
            String alias = p.getValue();
            if (alias != null) {
                appendable.append(" AS ").append(convertKeywords(alias));
            }
        }

        TableReferences from = node.getTables();
        if (from != null) {
            appendable.append(" FROM ");
            from.accept(this);
        }

        Expression where = node.getWhere();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }

        GroupBy group = node.getGroup();
        if (group != null) {
            appendable.append(' ');
            group.accept(this);
        }

        Expression having = node.getHaving();
        if (having != null) {
            appendable.append(" HAVING ");
            having.accept(this);
        }

        OrderBy order = node.getOrder();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }

        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }

        switch (option.lockMode) {
        case FOR_UPDATE:
            appendable.append(" FOR UPDATE");
            break;
        case LOCK_IN_SHARE_MODE:
            appendable.append(" LOCK IN SHARE MODE");
            break;
        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unknown option for SELECT: " + option);
        }
    }

    @Override
    public void visit(DMLSelectUnionStatement node) {

        if (node.isExplain()) {
            appendable.append("EXPLAIN ");
        }

        List<DMLSelectStatement> list = node.getSelectStmtList();
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("SELECT UNION must have at least one SELECT");
        }
        final int fstDist = node.getFirstDistinctIndex();
        int i = 0;
        for (DMLSelectStatement select : list) {
            if (i > 0) {
                appendable.append(" UNION ");
                if (i > fstDist) {
                    appendable.append("ALL ");
                } else {
                    appendable.append("DISTINCT ");
                }
                // 在 Union中，从第2个子句才能给SelectStmt添加括号
                appendable.append('(');
                select.accept(this);
                appendable.append(')');
            } else {
                select.accept(this);
            }

            ++i;
        }
        OrderBy order = node.getOrderBy();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    @Override
    public void visit(DMLUpdateStatement node) {
        appendable.append("UPDATE ");
        if (node.isLowPriority()) {
            appendable.append("LOW_PRIORITY ");
        }

        if (node.isCommitOnSuccess()) {
            appendable.append("COMMIT_ON_SUCCESS ");
        }

        if (node.isRollbackOnFail()) {
            appendable.append("ROLLBACK_ON_FAIL ");
        }

        if (node.isQueueOnPk()) {
            appendable.append("QUEUE_ON_PK ");

            if (node.getQueueOnPkNumP() != null) {
                node.getQueueOnPkNumP().accept(this);
            } else {
                appendable.append(String.valueOf(node.getQueueOnPkNum()));
            }

            appendable.append(" ");

        }

        if (node.isTargetAffectRow()) {
            appendable.append("TARGET_AFFECT_ROW ");

            if (node.getNumP() != null) {
                node.getNumP().accept(this);
            } else {
                appendable.append(String.valueOf(node.getNum()));
            }

            appendable.append(" ");

        }

        if (node.isIgnore()) {
            appendable.append("IGNORE ");
        }

        node.getTableRefs().accept(this);
        appendable.append(" SET ");
        boolean isFst = true;
        for (Pair<Identifier, Expression> p : node.getValues()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            p.getKey().accept(this);
            appendable.append(" = ");
            Expression value = p.getValue();
            boolean paren = value.getPrecedence() <= Expression.PRECEDENCE_COMPARISION;
            if (paren) {
                appendable.append('(');
            }
            value.accept(this);
            if (paren) {
                appendable.append(')');
            }
        }
        Expression where = node.getWhere();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
        OrderBy order = node.getOrderBy();
        if (order != null) {
            appendable.append(' ');
            order.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(" LIMIT ");
            Object size = limit.getSize();
            if (size instanceof ParamMarker) {
                ((ParamMarker) size).accept(this);
            } else {
                appendable.append(String.valueOf(size));
            }
        }
    }

    @Override
    public void visit(DDLTruncateStatement node) {
        appendable.append("TRUNCATE TABLE ");
        processTableName(node.getTable());
    }

    @Override
    public void visit(AddColumn addColumn) {
        appendable.append("ADD COLUMN ");
        addColumn.getColumnName().accept(this);
        appendable.append(" ");
        addColumn.getColumnDefine().accept(this);
        if (addColumn.isFirst()) {
            appendable.append(" FIRST");
        } else if (addColumn.getAfterColumn() != null) {
            appendable.append(" AFTER ");
            addColumn.getAfterColumn().accept(this);
        }
    }

    @Override
    public void visit(AddColumns addColumns) {
        appendable.append("ADD COLUMN ");
        boolean isFst = true;
        for (Pair<Identifier, ColumnDefinition> columnDefinitionPair : addColumns.getColumns()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }

            columnDefinitionPair.getKey().accept(this);
            appendable.append(" ");
            columnDefinitionPair.getValue().accept(this);
        }
    }

    @Override
    public void visit(AddIndex addIndex) {
        appendable.append("ADD INDEX ");
        if (addIndex.getIndexName() != null) {
            addIndex.getIndexName().accept(this);
        }
        appendable.append(" ");
        addIndex.getIndexDef().accept(this);
    }

    @Override
    public void visit(IndexDefinition indexDefinition) {
        if (indexDefinition.getIndexType() != null) {
            visit(indexDefinition.getIndexType());
        }
        appendable.append(" (");
        boolean isFst = true;
        for (IndexColumnName indexColumnName : indexDefinition.getColumns()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            indexColumnName.accept(this);
        }
        appendable.append(") ");
        isFst = true;
        for (IndexOption indexOption : indexDefinition.getOptions()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(" ");
            }
            indexOption.accept(this);
        }
    }

    @Override
    public void visit(IndexColumnName node) {
        /* col_name */
        node.getColumnName().accept(this);
        /* [(length)] */
        if (node.getLength() != null) {
            appendable.append("(");
            visit((LiteralNumber) node.getLength());
            appendable.append(") ");
        }
        /* [ASC | DESC] */
        if (node.isAsc() != null) {
            if (node.isAsc()) {
                appendable.append(" ASC ");
            } else {
                appendable.append(" DESC ");
            }
        }
    }

    @Override
    public void visit(AddFullTextIndex addFullTextIndex) {
        appendable.append("ADD FULLTEXT ");
        if (addFullTextIndex.isHasIndexType()) {
            appendable.append("INDEX ");
        }
        if (addFullTextIndex.getIndexName() != null) {
            addFullTextIndex.getIndexName().accept(this);
        }
        appendable.append(" ");
        addFullTextIndex.getIndexDef().accept(this);
    }

    /**
     * ADD [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
     * [index_option] ...
     */
    @Override
    public void visit(AddPrimaryKey addPrimaryKey) {
        appendable.append("ADD ");
        if (addPrimaryKey.getConstraint() != null) {
            appendable.append("CONSTRAINT ");
            if (!addPrimaryKey.getConstraint().getIdTextUnescape().equals("")) {
                addPrimaryKey.getConstraint().accept(this);
            }
            appendable.append(" ");
        }
        appendable.append("PRIMARY KEY ");
        addPrimaryKey.getIndexDef().accept(this);
    }

    @Override
    public void visit(AddForeignKey addForeignKey) {
        appendable.append("ADD ");
        if (addForeignKey.getConstraint() != null) {
            appendable.append("CONSTRAINT ");
            /* alter 允许出现 [CONSTRAINT [symbol] */
            if (!addForeignKey.getConstraint().getIdTextUnescape().equals("")) {
                addForeignKey.getConstraint().accept(this);
            }
            appendable.append(" ");
        }
        appendable.append("FOREIGN KEY ");
        if (addForeignKey.getIndexName() != null) {
            addForeignKey.getIndexName().accept(this);
            appendable.append(" ");
        }
        addForeignKey.getIndexDef().accept(this);
        addForeignKey.getReferenceDefinition().accept(this);
    }

    @Override
    public void visit(AddSpatialIndex addSpatialIndex) {
        appendable.append("ADD SPATIAL ");
        if (addSpatialIndex.isHasIndexType()) {
            appendable.append("INDEX ");
        }
        if (addSpatialIndex.getIndexName() != null) {
            addSpatialIndex.getIndexName().accept(this);
        }
        appendable.append(" ");
        addSpatialIndex.getIndexDef().accept(this);
    }

    @Override
    public void visit(AddUniqueKey addUniqueKey) {
        appendable.append("ADD ");
        if (addUniqueKey.getConstraint() != null) {
            appendable.append("CONSTRAINT ");
            /* alter 允许出现 [CONSTRAINT [symbol] */
            if (!addUniqueKey.getConstraint().getIdTextUnescape().equals("")) {
                addUniqueKey.getConstraint().accept(this);
            }
            appendable.append(" ");
        }
        appendable.append("UNIQUE ");

        // INDEX|KEY
        if (addUniqueKey.isHasIndexType()) {
            appendable.append("INDEX ");
        }

        // index_name
        if (addUniqueKey.getIndexName() != null) {
            addUniqueKey.getIndexName().accept(this);
        }
        appendable.append(" ");
        addUniqueKey.getIndexDef().accept(this);
    }

    @Override
    public void visit(AlterColumnDefaultVal alterColumnDefaultVal) {
        appendable.append(" ALTER COLUMN ");
        alterColumnDefaultVal.getColumnName().accept(this);
        appendable.append(" ");
        if (alterColumnDefaultVal.isDropDefault()) {
            appendable.append("DROP DEFAULT");
        } else {
            appendable.append("SET DEFAULT ");
            alterColumnDefaultVal.getDefaultValue().accept(this);
        }
    }

    @Override
    public void visit(ChangeColumn changeColumn) {
        appendable.append(" CHANGE COLUMN ");
        changeColumn.getOldName().accept(this);
        appendable.append(" ");
        changeColumn.getNewName().accept(this);
        appendable.append(" ");
        changeColumn.getColDef().accept(this);
        if (changeColumn.isFirst() && changeColumn.getNewName() != null) {
            appendable.append(" FIRST ");
        } else if (changeColumn.getAfterColumn() != null) {
            appendable.append(" AFTER ");
            changeColumn.getAfterColumn().accept(this);
        }
    }

    @Override
    public void visit(ModifyColumn modifyColumn) {
        appendable.append("MODIFY COLUMN ");
        modifyColumn.getColName().accept(this);
        appendable.append(" ");
        modifyColumn.getColDef().accept(this);
        if (modifyColumn.isFirst()) {
            appendable.append(" FIRST");
        } else {
            if (modifyColumn.getAfterColumn() != null) {
                appendable.append(" AFTER ");
                modifyColumn.getAfterColumn().accept(this);
            }
        }
    }

    @Override
    public void visit(DropColumn dropColumn) {
        appendable.append(" DROP COLUMN ");
        dropColumn.getColName().accept(this);
    }

    @Override
    public void visit(DropIndex dropIndex) {
        appendable.append(" DROP INDEX ");
        dropIndex.getIndexName().accept(this);
    }

    @Override
    public void visit(DropPrimaryKey dropPrimaryKey) {
        appendable.append("DROP PRIMARY KEY");
    }

    @Override
    public void visit(DropForeignKey foreignKey) {
        appendable.append("DROP FOREIGN KEY ");
        foreignKey.getFk_symbol().accept(this);
    }

    @Override
    public void visit(AlterSpecification node) {
        node.accept(this);
    }

    @Override
    public void visit(DDLAlterTableStatement node) {
        /* 用于executor的recoredSql和单库下推的情形 */
        appendable.append("ALTER ");
        if (node.isIgnore()) {
            appendable.append("IGNORE ");
        }
        appendable.append("TABLE ");
        processTableName(node.getTable());
        appendable.append(" ");

        /* alter_specification */
        CommerSpliter spSpliter = new CommerSpliter(appendable);
        for (AlterSpecification alterSpecification : node.getAlters()) {
            spSpliter.split();
            alterSpecification.accept(this);
        }

        /* tableOptions */
        if (node.getTableOptions() != null) {
            StringBuilder stringBuilder = new StringBuilder();
            MySQLOutputASTVisitor tableOptionVisitor = new MySQLOutputASTVisitor(stringBuilder);
            node.getTableOptions().accept(tableOptionVisitor);

            if (!stringBuilder.toString().isEmpty()) {
                spSpliter.split(); // 这里才可以判断是否应该加,号
                appendable.append(stringBuilder.toString());
            }
        }
    }

    @Override
    public void visit(DDLCreateIndexStatement node) {
        appendable.append("CREATE ");
        if (node.getConstraintType() != null) {
            appendable.append(node.getConstraintType().name());
        }
        appendable.append(" INDEX ");
        node.getIndexName().accept(this);
        appendable.append(" ");
        if (node.getIndexType() != null) {
            visit(node.getIndexType());
        }
        appendable.append(" ON ");
        processTableName(node.getTable());
        appendable.append(" ( ");

        boolean isFst = true;
        for (IndexColumnName indexColumnName : node.getColumns()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            indexColumnName.accept(this);
        }
        appendable.append(") ");
        if (node.getOptions() != null) {
            for (IndexOption indexOption : node.getOptions()) {
                appendable.append(" ");
                indexOption.accept(this);
            }
        }

        if (node.getAlgorithm() != null) {
            appendable.append(" ");
            node.getAlgorithm().accept(this);
        }

        if (node.getLock() != null) {
            appendable.append(" ");
            node.getLock().accept(this);
        }
    }

    @Override
    public void visit(ColumnDefinition columnDefinition) {
        /* data_type */
        DataTypeName dataTypeName = columnDefinition.getDataType().getTypeName();
        appendable.append(dataTypeName.name());

        if (DataType.DataTypeName.ENUM.equals(dataTypeName) || DataType.DataTypeName.SET.equals(dataTypeName)) {

            // add collection vals for ENUM type or Set type
            // e.g. ENUM('va1', 'val2', ...) or SET('var1', 'val2', ...)
            if (columnDefinition.getDataType().getCollectionVals().size() > 0) {
                appendable.append("(");
                int collectionCount = columnDefinition.getDataType().getCollectionVals().size();
                for (int i = 0; i < collectionCount; i++) {
                    if (i > 0) {
                        appendable.append(",");
                    }
                    Expression collExpression = columnDefinition.getDataType().getCollectionVals().get(i);
                    collExpression.accept(this);
                }
                appendable.append(")");
                if (columnDefinition.getDataType().getCharSet() != null) {
                    appendable.append(" CHARACTER SET ");
                    columnDefinition.getDataType().getCharSet().accept(this);
                    appendable.append(' ');
                }
                if (columnDefinition.getDataType().getCollation() != null) {
                    appendable.append(" COLLATE ");
                    columnDefinition.getDataType().getCollation().accept(this);
                    appendable.append(' ');
                }
            }
        } else {
            // add length vals for type
            // e.g. INT( length )
            if (columnDefinition.getDataType().getLength() != null) {
                appendable.append("(");
                LiteralNumber number = (LiteralNumber) columnDefinition.getDataType().getLength();
                if (number != null) {
                    appendable.append(number.getNumber());
                }
                LiteralNumber decimals = (LiteralNumber) columnDefinition.getDataType().getDecimals();
                if (decimals != null) {
                    appendable.append(',');
                    appendable.append(decimals.getNumber());
                }
                appendable.append(")");
            }
            if (columnDefinition.getDataType().isBinary()) {
                appendable.append(" BINARY ");
            }
            if (columnDefinition.getDataType().isUnsigned()) {
                appendable.append(" UNSIGNED ");
            }
            if (columnDefinition.getDataType().isZerofill()) {
                appendable.append(" ZEROFILL ");
            }
            if (columnDefinition.getDataType().getCharSet() != null) {
                appendable.append(" CHARACTER SET ");
                columnDefinition.getDataType().getCharSet().accept(this);
                appendable.append(' ');
            }
            if (columnDefinition.getDataType().getCollation() != null) {
                appendable.append(" COLLATE ");
                columnDefinition.getDataType().getCollation().accept(this);
                appendable.append(' ');
            }
            if (columnDefinition.getDataType().getFsp() != null) {
                appendable.append("(");
                columnDefinition.getDataType().getFsp().accept(this);
                appendable.append(")");
            }
        }

        appendable.append(" ");

        /* [NOT NULL | NULL] */
        if (columnDefinition.getNotNull() != null) {
            if (columnDefinition.getNotNull() == ColumnDefinition.ColumnNull.NULL) {
                appendable.append("NULL ");
            } else {
                appendable.append("NOT NULL ");
            }
        }

        /* [DEFAULT default_value] */
        if (columnDefinition.getDefaultVal() != null) {
            appendable.append("DEFAULT ");
            columnDefinition.getDefaultVal().accept(this);
            appendable.append(" ");
        }

        /* ON UPDATE CURRENT_TIMESTAMP */
        if (columnDefinition.isOnUpdateCurrentTimestamp()) {
            appendable.append("ON UPDATE CURRENT_TIMESTAMP ");
        }

        /* [AUTO_INCREMENT] */
        if (columnDefinition.isAutoIncrement()) {
            appendable.append("AUTO_INCREMENT ");
        }

        // We can't do this since the output column definition will be executed
        // on native MySQL that doesnt' support such syntax.
        /* [AUTO_INCREMENT [BY GROUP | SIMPLE | TIME]] */
        // if (columnDefinition.getAutoIncrementType() != Type.NA) {
        // appendable.append("BY ");
        // switch (columnDefinition.getAutoIncrementType()) {
        // case SIMPLE:
        // appendable.append("SIMPLE ");
        // break;
        // case TIME:
        // appendable.append("TIME ");
        // break;
        // case GROUP:
        // default:
        // appendable.append("GROUP ");
        // break;
        // }
        // }

        /* [UNIQUE [KEY] | [PRIMARY] KEY] */
        if (columnDefinition.getSpecialIndex() != null) {
            if (columnDefinition.getSpecialIndex() == SpecialIndex.PRIMARY) {
                appendable.append("PRIMARY KEY ");
            } else if (columnDefinition.getSpecialIndex() == SpecialIndex.UNIQUE) {
                appendable.append("UNIQUE ");
            }
        }

        /* [COMMENT 'string'] */
        if (columnDefinition.getComment() != null) {
            appendable.append(" COMMENT ");
            /* 处理LiterString时会自动加上'' */
            columnDefinition.getComment().accept(this);
            appendable.append(' ');
        }

        /* [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}] */
        if (columnDefinition.getColumnFormat() != null) {
            appendable.append("COLUMN_FORMAT ");
            if (columnDefinition.getColumnFormat() == ColumnDefinition.ColumnFormat.FIXED) {
                appendable.append("FIXED ");
            } else if (columnDefinition.getColumnFormat() == ColumnDefinition.ColumnFormat.DYNAMIC) {
                appendable.append("DYNAMIC ");
            } else if (columnDefinition.getColumnFormat() == ColumnDefinition.ColumnFormat.DEFAULT) {
                appendable.append("DEFAULT ");
            }
        }

        /* [STORAGE {DISK|MEMORY|DEFAULT}] */
        if (columnDefinition.getStorage() != null) {
            appendable.append("STORAGE ");
            if (columnDefinition.getStorage() == ColumnDefinition.Storage.DISK) {
                appendable.append("DISK ");
            } else if (columnDefinition.getStorage() == ColumnDefinition.Storage.MEMORY) {
                appendable.append("MEMORY ");
            } else if (columnDefinition.getStorage() == ColumnDefinition.Storage.DEFAULT) {
                appendable.append("DEFAULT ");
            }
        }

        /* reference_definition */
        if (columnDefinition.getReferenceDefinition() != null) {
            columnDefinition.getReferenceDefinition().accept(this);
        }

        /* TODO: [RESTRICT | CASCADE | SET NULL | NO ACTION] */
    }

    @Override
    public void visit(DBPartitionBy DBPartitionBy) {
        appendable.append(" DBPARTITION BY ");
        if (DBPartitionBy.getType() == PartitionByType.HASH) {
            appendable.append("HASH(");
            if (DBPartitionBy.getColExpr() != null) {
                DBPartitionBy.getColExpr().accept(this);
            }
            appendable.append(")");
        }
    }

    @Override
    public void visit(TBPartitionBy TBPartitionBy) {
        appendable.append(" TBPARTITION BY ");
        if (TBPartitionBy.getType() == PartitionByType.HASH) {
            appendable.append("HASH(");
            if (TBPartitionBy.getColExpr() != null) {
                TBPartitionBy.getColExpr().accept(this);
            }
            appendable.append(")");
        }
    }

    @Override
    public void visit(SubpartitionDefinition subpartitionDefinition) {
        appendable.append(" SUBPARTITION ");
        if (subpartitionDefinition.getLogicalName() != null) {
            subpartitionDefinition.getLogicalName().accept(this);
            appendable.append(' ');
        }

        // [[STORAGE] ENGINE [=] engine_name]
        if (subpartitionDefinition.getEngineName() != null) {
            if (subpartitionDefinition.isStorage()) {
                appendable.append("STORAGE ");
            }
            subpartitionDefinition.getEngineName().accept(this);
        }
        // [COMMENT [=] 'comment_text' ]
        if (subpartitionDefinition.getCommentText() != null) {
            appendable.append("COMMENT = ");
            subpartitionDefinition.getCommentText().accept(this);
            appendable.append(" ");
        }
        // [DATA DIRECTORY [=] 'data_dir']
        if (subpartitionDefinition.getDataDir() != null) {
            appendable.append("DATA DIRECTORY = ");
            subpartitionDefinition.getDataDir().accept(this);
            appendable.append(" ");
        }
        // [INDEX DIRECTORY [=] 'index_dir']
        if (subpartitionDefinition.getIndexDir() != null) {
            appendable.append("INDEX DIRECTORY = ");
            subpartitionDefinition.getIndexDir().accept(this);
            appendable.append(" ");
        }
        // [MAX_ROWS [=] max_number_of_rows]
        if (subpartitionDefinition.getMaxNumberOfRows() != null) {
            appendable.append("MAX_ROWS = ");
            subpartitionDefinition.getMaxNumberOfRows().accept(this);
            appendable.append(" ");
        }
        // [MIN_ROWS [=] min_number_of_rows]
        if (subpartitionDefinition.getMinNumberOfRows() != null) {
            appendable.append("MIN_ROWS = ");
            subpartitionDefinition.getMinNumberOfRows().accept(this);
            appendable.append(" ");
        }
        // [TABLESPACE [=] tablespace_name]
        if (subpartitionDefinition.getTablespaceName() != null) {
            appendable.append("TABLESPACE = ");
            subpartitionDefinition.getTablespaceName().accept(this);
            appendable.append(" ");
        }
        // [NODEGROUP [=] node_group_id]
        if (subpartitionDefinition.getNodeGroupId() != null) {
            appendable.append("NODEGROUP = ");
            subpartitionDefinition.getNodeGroupId().accept(this);
        }
    }

    @Override
    public void visit(PartitionDefinition partitionDefinition) {
        appendable.append(" PARTITION ");
        if (partitionDefinition.getPartitionName() != null) {
            partitionDefinition.getPartitionName().accept(this);
            appendable.append(' ');
        }

        /**
         * [VALUES {LESS THAN {(expr | value_list) | MAXVALUE} | IN (value_list)}]
         */
        if (partitionDefinition.isHasValues()) {
            /*
             * PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2013-01-01 00:00:00')),
             */
            appendable.append(" VALUES ");

            switch (partitionDefinition.getPartitionDefinitionValuesType()) {
            case LESSTHAN_EXPR:
                appendable.append("LESS THAN (");
                if (partitionDefinition.getValuesLessThanExpr() != null) {
                    if (partitionDefinition.getValuesLessThanExpr() instanceof Identifier
                        && ((Identifier) partitionDefinition.getValuesLessThanExpr()).getIdTextUpUnescape()
                        .equals("MAXVALUE")) {
                        appendable.append("MAXVALUE");
                    } else {
                        partitionDefinition.getValuesLessThanExpr().accept(this);
                    }
                }
                appendable.append(") ");
                break;
            case LESSTHAN_VALUELIST:
                appendable.append("LESS THAN (");
                if (partitionDefinition.getValueLessThanValueList() != null) {
                    CommerSpliter valueListSpliter = new CommerSpliter(appendable);
                    for (Expression value : partitionDefinition.getValueLessThanValueList()) {
                        valueListSpliter.split();
                        if (value instanceof Identifier
                            && ((Identifier) value).getIdTextUpUnescape().equals("MAXVALUE")) {
                            appendable.append("MAXVALUE");
                        } else {
                            value.accept(this);
                        }
                    }
                }
                appendable.append(") ");
                break;
            case LESSTHAN_MAXVALUE:
                appendable.append("LESS THAN (MAXVALUE) ");
                break;
            case IN:
                appendable.append("IN (");
                if (partitionDefinition.getValuesInValueList() != null) {
                    CommerSpliter valueListSpliter = new CommerSpliter(appendable);
                    for (Expression value : partitionDefinition.getValuesInValueList()) {
                        valueListSpliter.split();
                        value.accept(this);
                    }
                }
                appendable.append(")");
                break;
            }
        }

        // [[STORAGE] ENGINE [=] engine_name]
        if (partitionDefinition.getEngineName() != null) {
            if (partitionDefinition.isHasStorage()) {
                appendable.append("STORAGE ");
            }
            partitionDefinition.getEngineName().accept(this);
        }
        // [COMMENT [=] 'comment_text' ]
        if (partitionDefinition.getCommentText() != null) {
            appendable.append("COMMENT = ");
            partitionDefinition.getCommentText().accept(this);
            appendable.append(" ");
        }
        // [DATA DIRECTORY [=] 'data_dir']
        if (partitionDefinition.getDataDir() != null) {
            appendable.append("DATA DIRECTORY = ");
            partitionDefinition.getDataDir().accept(this);
            appendable.append(" ");
        }
        // [INDEX DIRECTORY [=] 'index_dir']
        if (partitionDefinition.getIndexDir() != null) {
            appendable.append("INDEX DIRECTORY = ");
            partitionDefinition.getIndexDir().accept(this);
            appendable.append(" ");
        }
        // [MAX_ROWS [=] max_number_of_rows]
        if (partitionDefinition.getMaxNumberOfRows() != null) {
            appendable.append("MAX_ROWS = ");
            partitionDefinition.getMaxNumberOfRows().accept(this);
            appendable.append(" ");
        }
        // [MIN_ROWS [=] min_number_of_rows]
        if (partitionDefinition.getMinNumberOfRows() != null) {
            appendable.append("MIN_ROWS = ");
            partitionDefinition.getMinNumberOfRows().accept(this);
            appendable.append(" ");
        }
        // [TABLESPACE [=] tablespace_name]
        if (partitionDefinition.getTablespaceName() != null) {
            appendable.append("TABLESPACE = ");
            partitionDefinition.getTablespaceName().accept(this);
            appendable.append(" ");
        }
        // [NODEGROUP [=] node_group_id]
        if (partitionDefinition.getNodeGroupId() != null) {
            appendable.append("NODEGROUP = ");
            partitionDefinition.getNodeGroupId().accept(this);
            appendable.append(" ");
        }
        // [(subpartition_definition [, subpartition_definition] ...)]
        if (partitionDefinition.getSubpartitionDefinitionList() != null) {
            CommerSpliter subpartitionSpliter = new CommerSpliter(appendable);
            appendable.append(" (");
            for (SubpartitionDefinition subpartitionDefinition : partitionDefinition.getSubpartitionDefinitionList()) {
                subpartitionSpliter.split();
                subpartitionDefinition.accept(this);
            }
            appendable.append(") ");
        }
    }

    @Override
    public void visit(PartitionOptions partitionOptions) {
        // PARTITION BY
        if (partitionOptions.getPartitionBy() != null) {
            partitionOptions.getPartitionBy().accept(this);
            appendable.append(' ');
        }
        // [PARTITIONS num]
        if (partitionOptions.getNum() != null) {
            appendable.append("PARTITIONS ");
            partitionOptions.getNum().accept(this);
            appendable.append(' ');
        }
        // SUBPARTITION BY
        if (partitionOptions.getSubPartitionBy() != null) {
            partitionOptions.getSubPartitionBy().accept(this);
            appendable.append(' ');
        }
        // [(partition_definition [, partition_definition] ...)]
        if (partitionOptions.getPartitionDefinitionList() != null) {
            CommerSpliter partitionDefinitionSpliter = new CommerSpliter(appendable);
            appendable.append(" (");
            for (PartitionDefinition partitionDefinition : partitionOptions.getPartitionDefinitionList()) {
                partitionDefinitionSpliter.split();
                partitionDefinition.accept(this);
            }
            appendable.append(") ");
        }
    }

    @Override
    public void visit(SubPartitionBy subPartitionBy) {
        appendable.append(" SUBPARTITION BY ");
        if (subPartitionBy.isLiner()) {
            appendable.append("LINEAR ");
        }

        if (subPartitionBy.getSubPartitionByType() != null) {
            switch (subPartitionBy.getSubPartitionByType()) {
            case HASH:
                appendable.append("HASH (");
                if (subPartitionBy.getHashExpr() != null) {
                    subPartitionBy.getHashExpr().accept(this);
                }
                appendable.append(") ");
                break;
            case KEY:
                appendable.append("KEY ");
                if (subPartitionBy.getAlgorithm() != null) {
                    appendable.append(" ALGORITHM=");
                    subPartitionBy.getAlgorithm().accept(this);
                }
                appendable.append(" (");
                if (subPartitionBy.getColumnList() != null) {
                    CommerSpliter columnSpliter = new CommerSpliter(appendable);
                    for (Identifier column : subPartitionBy.getColumnList()) {
                        columnSpliter.split();
                        column.accept(this);
                    }
                }
                appendable.append(") ");
                break;
            }
        }

        if (subPartitionBy.getNum() != null) {
            appendable.append("SUBPARTITIONS ");
            subPartitionBy.getNum().accept(this);
        }
    }

    @Override
    public void visit(PartitionBy partitionBy) {
        appendable.append(" PARTITION BY ");
        if (partitionBy.getPartitionByType() != null) {
            switch (partitionBy.getPartitionByType()) {
            case HASH:
                if (partitionBy.isLiner()) {
                    appendable.append("LINEAR ");
                }
                appendable.append("HASH (");
                if (partitionBy.getHashExpr() != null) {
                    partitionBy.getHashExpr().accept(this);
                }
                appendable.append(") ");
                break;
            case KEY:
                if (partitionBy.isLiner()) {
                    appendable.append("LINEAR ");
                }
                appendable.append("KEY ");
                if (partitionBy.getAlgorithm() != null) {
                    appendable.append("ALGORITHM= ");
                    partitionBy.getAlgorithm().accept(this);
                    appendable.append(' ');
                }
                if (partitionBy.getKeyColumnList() != null) {
                    appendable.append(" (");
                    CommerSpliter columnSpliter = new CommerSpliter(appendable);
                    for (Identifier column : partitionBy.getKeyColumnList()) {
                        columnSpliter.split();
                        column.accept(this);
                    }
                    appendable.append(") ");
                }
                break;
            case RANGE:
                appendable.append("RANGE ");
                if (partitionBy.getRangeExpr() != null) {
                    appendable.append(" (");
                    partitionBy.getRangeExpr().accept(this);
                    appendable.append(") ");
                } else if (partitionBy.getRangeColumnList() != null) {
                    appendable.append(" COLUMNS(");
                    CommerSpliter columnSpliter = new CommerSpliter(appendable);
                    for (Identifier column : partitionBy.getRangeColumnList()) {
                        columnSpliter.split();
                        column.accept(this);
                    }
                    appendable.append(") ");
                }
                break;
            case LIST:
                appendable.append("LIST ");
                if (partitionBy.getListExpr() != null) {
                    appendable.append(" (");
                    partitionBy.getListExpr().accept(this);
                    appendable.append(") ");
                } else if (partitionBy.getListColumnList() != null) {
                    appendable.append(" COLUMNS(");
                    CommerSpliter columnSpliter = new CommerSpliter(appendable);
                    for (Identifier column : partitionBy.getListColumnList()) {
                        columnSpliter.split();
                        column.accept(this);
                    }
                    appendable.append(") ");
                }
                break;
            }
        }
    }

    @Override
    public void visit(DBPartitionOptions DBPartitionOptions) {
        if (DBPartitionOptions.getDbpartitionBy() != null) {
            DBPartitionOptions.getDbpartitionBy().accept(this);
        }

        if (DBPartitionOptions.getDbpartitions() > 0) {
            appendable.append(" DBPARTITIONS ");
            appendable.append(DBPartitionOptions.getDbpartitions()).append(" ");
        }

        if (DBPartitionOptions.getTbpartitionBy() != null) {
            DBPartitionOptions.getTbpartitionBy().accept(this);
        }

        if (DBPartitionOptions.getTbpartitions() > 0) {
            appendable.append(" TBPARTITIONS ");
            appendable.append(DBPartitionOptions.getTbpartitions()).append(" ");
        }
    }

    @Override
    public void visit(DDLCreateTableStatement node) {
        /* CREATE */
        appendable.append("CREATE ");
        /* [TEMPORARY] */
        if (node.isTemporary()) {
            appendable.append("TEMPORARY ");
        }
        /* TABLE */
        appendable.append("TABLE ");

        /* [IF NOT EXISTS] */
        if (node.isIfNotExists()) {
            appendable.append("IF NOT EXISTS ");
        }

        /* tbl_name */
        processTableName(node.getTable());

        /**
         * 这里还是用原始的sql column defs而不用ColumnMeta，因为columnMeta是DataType, 不能够还原成标准的sql串
         */
        // TableMeta tableMeta = createTable.getTableMeta();
        List<Pair<Identifier, ColumnDefinition>> colDefs = node.getColDefs();

        /* ( */
        if (colDefs.size() > 0) {
            appendable.append('(');
        }

        /* create_definition */

        /**
         * cdSpliter是create_definition内的spliter
         */
        CommerSpliter cdSpliter = new CommerSpliter(appendable);

        /* 列信息 */
        for (Pair<Identifier, ColumnDefinition> columnMeta : colDefs) {
            cdSpliter.split();
            /* col_name */
            columnMeta.getKey().accept(this);
            appendable.append(' ');
            /* column_definition */
            columnMeta.getValue().accept(this);
        }

        /*
         * 主键 [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...)
         * [index_option]...
         */

        if (node.getPrimaryKey() != null) {
            /* [CONSTRAINT [symbol]] */
            cdSpliter.split();
            if (node.isHasPrimaryKeyConstraint()) {
                appendable.append("CONSTRAINT ");
            }
            if (node.getPrimaryKeyConstraint() != null) {
                if (!node.getPrimaryKeyConstraint().getIdTextUnescape().equals("")) {
                    node.getPrimaryKeyConstraint().accept(this);
                }
                appendable.append(' ');
            }
            /* PRIMARY KEY */
            appendable.append("PRIMARY KEY ");
            /* [index_type] */
            if (node.getPrimaryKey().getIndexType() != null) {
                this.visit(node.getPrimaryKey().getIndexType());
            }

            /* (index_col_name,...) */
            appendable.append("(");
            /* primary key内的spliter */
            CommerSpliter pkSpliter = new CommerSpliter(appendable);
            if (node.getPrimaryKey().getColumns() != null && node.getPrimaryKey().getColumns().size() > 0) {
                for (IndexColumnName columnName : node.getPrimaryKey().getColumns()) {
                    pkSpliter.split();
                    /* col_name [(length)] [ASC | DESC] */
                    columnName.accept(this);
                }
            }
            appendable.append(")");

            /* [index_option] ... */
            if (node.getPrimaryKey().getOptions() != null && node.getPrimaryKey().getOptions().size() > 0) {
                for (IndexOption indexOption : node.getPrimaryKey().getOptions()) {
                    appendable.append(" ");
                    indexOption.accept(this);
                }
            }
        }

        /*
         * {INDEX|KEY} [index_name] [index_type] (index_col_name,...) [index_option] ...
         */
        if (node.getKeys() != null) {
            for (Pair<Identifier, IndexDefinition> indexDefinition : node.getKeys()) {
                cdSpliter.split();
                /* {INDEX|KEY} */
                appendable.append("INDEX ");
                /* [index_name] */
                if (indexDefinition.getKey() != null) {
                    indexDefinition.getKey().accept(this);
                    appendable.append(' ');
                }
                /* [index_type] */
                if (indexDefinition.getValue().getIndexType() != null) {
                    this.visit(indexDefinition.getValue().getIndexType());
                }
                /* (index_col_name,...) */
                appendable.append("(");
                /* index key spliter */
                CommerSpliter idxSpliter = new CommerSpliter(appendable);
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getColumns() != null) {
                    for (IndexColumnName columnName : indexDefinition.getValue().getColumns()) {
                        idxSpliter.split();
                        /* col_name [(length)] [ASC | DESC] */
                        columnName.accept(this);
                    }
                }
                appendable.append(")");

                /* [index_option]... */
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getOptions() != null) {
                    for (IndexOption indexOption : indexDefinition.getValue().getOptions()) {
                        appendable.append(" ");
                        indexOption.accept(this);
                    }
                }
            }

        }

        /* KEY is normally a synonym for INDEX. */

        /*
         * [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name] [index_type]
         * (index_col_name,...) [index_option] ...
         */
        if (node.getUniqueKeys() != null) {
            for (Pair<Identifier, IndexDefinition> indexDefinition : node.getUniqueKeys()) {
                /* [CONSTRAINT [symbol]] */
                cdSpliter.split();

                if (indexDefinition.getValue().isHasConstraint()) {
                    appendable.append("CONSTRAINT ");
                }
                if (indexDefinition.getValue().getUniqueConstraint() != null) {
                    if (!indexDefinition.getValue().getUniqueConstraint().getIdTextUnescape().equals("")) {
                        indexDefinition.getValue().getUniqueConstraint().accept(this);
                    }
                    appendable.append(' ');
                }

                appendable.append("UNIQUE INDEX ");
                /* [index_name] */
                if (indexDefinition.getKey() != null) {
                    indexDefinition.getKey().accept(this);
                    appendable.append(' ');
                }
                /* [index_type] */
                if (indexDefinition.getValue().getIndexType() != null) {
                    this.visit(indexDefinition.getValue().getIndexType());
                }
                /* (index_col_name,...) */
                appendable.append("(");
                CommerSpliter ukSpliter = new CommerSpliter(appendable);
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getColumns() != null) {
                    for (IndexColumnName columnName : indexDefinition.getValue().getColumns()) {
                        ukSpliter.split();
                        /* col_name [(length)] [ASC | DESC] */
                        columnName.accept(this);
                    }
                }
                appendable.append(")");

                /* [index_option]... */
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getOptions() != null) {
                    for (IndexOption indexOption : indexDefinition.getValue().getOptions()) {
                        appendable.append(" ");
                        indexOption.accept(this);
                    }
                }
            }
        }

        /**
         * | {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (index_col_name,...)
         * [index_option] ...
         */
        if (node.getFullTextKeys() != null) {
            for (Pair<Identifier, IndexDefinition> indexDefinition : node.getFullTextKeys()) {
                cdSpliter.split();

                appendable.append("FULLTEXT INDEX ");

                /* [index_name] */
                if (indexDefinition.getKey() != null) {
                    this.visit(indexDefinition.getKey());
                    appendable.append(' ');
                }

                /* (index_col_name,...) */
                appendable.append("(");
                CommerSpliter ftSpliter = new CommerSpliter(appendable);
                for (IndexColumnName columnName : indexDefinition.getValue().getColumns()) {
                    ftSpliter.split();
                    /* col_name [(length)] [ASC | DESC] */
                    columnName.accept(this);
                }
                appendable.append(")");

                /* [index_option] */
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getOptions() != null) {
                    for (IndexOption indexOption : indexDefinition.getValue().getOptions()) {
                        appendable.append(" ");
                        indexOption.accept(this);
                    }
                }
            }
        }

        if (node.getSpatialKeys() != null) {
            for (Pair<Identifier, IndexDefinition> indexDefinition : node.getSpatialKeys()) {
                cdSpliter.split();

                appendable.append("SPATIAL INDEX ");

                /* [index_name] */
                if (indexDefinition.getKey() != null) {
                    this.visit(indexDefinition.getKey());
                    appendable.append(' ');
                }

                /* (index_col_name,...) */
                appendable.append("(");
                CommerSpliter spSpliter = new CommerSpliter(appendable);
                for (IndexColumnName columnName : indexDefinition.getValue().getColumns()) {
                    spSpliter.split();
                    /* col_name [(length)] [ASC | DESC] */
                    columnName.accept(this);
                }
                appendable.append(")");

                /* [index_option] */
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getOptions() != null) {
                    for (IndexOption indexOption : indexDefinition.getValue().getOptions()) {
                        appendable.append(" ");
                        indexOption.accept(this);
                    }
                }
            }
        }

        /*
         * | [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name,...)
         * reference_definition
         */
        if (node.getForeignKeys() != null) {
            for (Pair<Identifier, IndexDefinition> indexDefinition : node.getForeignKeys()) {
                /* [CONSTRAINT [symbol]] */
                cdSpliter.split();

                if (indexDefinition.getValue().isHasConstraint()) {
                    appendable.append("CONSTRAINT ");
                }
                if (indexDefinition.getValue().getForeignKeyConstraint() != null) {
                    if (!indexDefinition.getValue().getForeignKeyConstraint().getIdTextUnescape().equals("")) {
                        indexDefinition.getValue().getForeignKeyConstraint().accept(this);
                    }
                    appendable.append(' ');
                }

                /* FOREIGN KEY */
                appendable.append("FOREIGN KEY ");

                /* [index_name] */
                if (indexDefinition.getKey() != null) {
                    this.visit(indexDefinition.getKey());
                    appendable.append(' ');
                }

                /* (index_col_name,...) */
                appendable.append("(");
                CommerSpliter fkSpliter = new CommerSpliter(appendable);
                for (IndexColumnName columnName : indexDefinition.getValue().getColumns()) {
                    fkSpliter.split();
                    /* col_name [(length)] [ASC | DESC] */
                    columnName.accept(this);
                }
                appendable.append(")");

                /* [index_option] ... */
                if (indexDefinition.getValue() != null && indexDefinition.getValue().getOptions() != null) {
                    for (IndexOption indexOption : indexDefinition.getValue().getOptions()) {
                        appendable.append(" ");
                        indexOption.accept(this);
                    }
                }

                if (indexDefinition.getValue().getForeignKeyReferenceDefinition() != null) {
                    indexDefinition.getValue().getForeignKeyReferenceDefinition().accept(this);
                }
            }
        }

        /* ) */
        if (colDefs.size() > 0) {
            appendable.append(')');
        }

        /* table_options */
        if (node.getTableOptions() != null) {
            node.getTableOptions().accept(this);
        }

        /* LIKE old_tbl_name */
        if (node.getOldTblName() != null) {
            appendable.append(" LIKE ");
            node.getOldTblName().accept(this);
        }

        // [partition_options]
        if (node.getPartitionOptions() != null) {
            appendable.append(' ');
            node.getPartitionOptions().accept(this);
        }

        /** select_statement */
        if (node.getSelect() != null) {
            appendable.append(' ');
            if (node.getSelect().getKey() != null) {
                switch (node.getSelect().getKey()) {
                case IGNORED:
                    appendable.append("IGNORE ");
                    break;
                case REPLACE:
                    appendable.append("REPLACE ");
                    break;
                }
            }
            if (node.getSelect().getValue() != null) {
                node.getSelect().getValue().accept(this);
            }
        }
    }

    @Override
    public void visit(DDLRenameTableStatement node) {
        appendable.append("RENAME TABLE ");
        boolean isFst = true;
        for (Pair<Identifier, Identifier> p : node.getList()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            processTableName(p.getKey());
            appendable.append(" TO ");
            processTableName(p.getValue());
        }
    }

    @Override
    public void visit(DDLDropIndexStatement node) {
        appendable.append("DROP INDEX ");
        node.getIndexName().accept(this);
        appendable.append(" ON ");
        processTableName(node.getTable());

        if (node.getAlgorithm() != null) {
            appendable.append(" ");
            node.getAlgorithm().accept(this);
        }

        if (node.getLock() != null) {
            appendable.append(" ");
            node.getLock().accept(this);
        }
    }

    @Override
    public void visit(DDLDropTableStatement node) {
        appendable.append("DROP ");
        if (node.isTemp()) {
            appendable.append("TEMPORARY ");
        }
        appendable.append("TABLE ");
        if (node.isIfExists()) {
            appendable.append("IF EXISTS ");
        }
        boolean isFst = true;
        for (Identifier arg : node.getTableNames()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(',');
            }
            processTableName(arg);
        }
        switch (node.getMode()) {
        case CASCADE:
            appendable.append(" CASCADE");
            break;
        case RESTRICT:
            appendable.append(" RESTRICT");
            break;
        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unsupported mode for DROP TABLE: " + node.getMode());
        }
    }

    @Override
    public void visit(ExtDDLCreatePolicy node) {
        appendable.append("CREATE POLICY ");
        node.getName().accept(this);
        appendable.append(" (");
        boolean first = true;
        for (Pair<Integer, Expression> p : node.getProportion()) {
            if (first) {
                first = false;
            } else {
                appendable.append(", ");
            }
            appendable.append(p.getKey()).append(' ');
            p.getValue().accept(this);
        }
        appendable.append(')');
    }

    @Override
    public void visit(ExtDDLDropPolicy node) {
        appendable.append("DROP POLICY ");
        node.getPolicyName().accept(this);
    }

    @Override
    public void visit(ShowTopology node) {
        appendable.append("SHOW TOPOLOGY ");
        node.getName().accept(this);
    }

    @Override
    public void visit(DMLLoadStatement node) {
        appendable.append("LOAD DATA ");

        switch (node.getMode()) {
        case CONCURRENT:
            appendable.append("CONCURRENT ");
            break;
        case LOW:
            appendable.append("LOW_PRIORITY ");
            break;

        case UNDEF:
            break;
        default:
            throw new IllegalArgumentException("unknown mode for LOAD DATA: " + node.getMode());
        }

        if (node.isLocal()) {
            appendable.append("LOCAL ");
        }

        appendable.append("INFILE ");

        node.getFileName().accept(this);
        appendable.append(" ");

        switch (node.getDuplicateMode()) {
        case IGNORE:
            appendable.append("IGNORE ");
            break;
        case REPLACE:
            appendable.append("REPLACE ");
            break;
        case UNDEF:
            break;

        default:
            throw new IllegalArgumentException("unknown mode for LOAD DATA: " + node.getDuplicateMode());
        }

        appendable.append("INTO TABLE ");
        processTableName(node.getTable());
        appendable.append(" ");
        if (node.getCharSet() != null) {
            appendable.append("CHARACTER SET ");
            appendable.append(node.getCharSet()).append(" ");
        }

        if (node.getFieldsEnclosedBy() != null || node.getFieldsEscapedBy() != null
            || node.getFiledsTerminatedBy() != null) {
            appendable.append("COLUMNS ");
            if (node.getFiledsTerminatedBy() != null) {
                appendable.append("TERMINATED BY ");
                node.getFiledsTerminatedBy().accept(this);
                appendable.append(" ");

            }

            if (node.getFieldsEnclosedBy() != null) {
                if (node.isOptionally()) {
                    appendable.append("OPTIONALLY ENCLOSED BY ");
                } else {
                    appendable.append("ENCLOSED BY ");
                }
                node.getFieldsEnclosedBy().accept(this);
                appendable.append(" ");
            }

            if (node.getFieldsEscapedBy() != null) {
                appendable.append("ESCAPED BY ");
                node.getFieldsEscapedBy().accept(this);
                appendable.append(" ");
            }

        }

        if (node.getLinesStartingBy() != null || node.getLinesTerminatedBy() != null) {
            appendable.append("LINES");

            if (node.getLinesStartingBy() != null) {
                appendable.append("STARTING BY ");
                node.getLinesStartingBy().accept(this);
                appendable.append(" ");
            }

            if (node.getLinesTerminatedBy() != null) {
                appendable.append("TERMINATED BY ");
                node.getLinesTerminatedBy().accept(this);
                appendable.append(" ");
            }
        }

        if (node.getIgnoreLines() != null) {
            appendable.append("IGNORE ");
            appendable.append(node.getIgnoreLines());
            appendable.append(" LINES ");
        }

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        if (node.getValues() != null) {
            appendable.append("SET ");
            boolean isFst = true;
            for (Pair<Identifier, Expression> p : node.getValues()) {
                if (isFst) {
                    isFst = false;
                } else {
                    appendable.append(", ");
                }
                p.getKey().accept(this);
                appendable.append(" = ");
                Expression value = p.getValue();
                boolean paren = value.getPrecedence() <= Expression.PRECEDENCE_COMPARISION;
                if (paren) {
                    appendable.append('(');
                }
                value.accept(this);
                if (paren) {
                    appendable.append(')');
                }
            }
        }
    }

    @Override
    public void visit(ShowPartitions node) {
        appendable.append("SHOW DBPARTITIONS ");
        node.getName().accept(this);
    }

    @Override
    public void visit(ShowBroadcasts showBroadcasts) {
        appendable.append("SHOW BROADCASTS");

    }

    @Override
    public void visit(ShowRule showRule) {
        appendable.append("SHOW RULE");
    }

    @Override
    public void visit(ShowRuleStatusStatement showRuleStatus) {
        appendable.append("SHOW RULE STATUS");
    }

    @Override
    public void visit(ShowDdlStatusStatement showDdlStatus) {
        appendable.append("SHOW DDL STATUS");
    }

    @Override
    public void visit(ReloadSchema reloadSchema) {
        appendable.append("RELOAD SCHEMA");
    }

    @Override
    public void visit(ReloadUsers reloadUsers) {
        appendable.append("RELOAD USERS");
    }

    @Override
    public void visit(DALPrepareStatement node) {
        appendable.append("PREPARE ");
        appendable.append(node.getStmt_id());
    }

    @Override
    public void visit(DALExecuteStatement node) {
        appendable.append("EXECUTE ");
        appendable.append(node.getStmt_id());
    }

    @Override
    public void visit(DALDeallocateStatement node) {
        appendable.append("DEALLOCATE PREPARE ");
        appendable.append(node.getStmt_id());
    }

    @Override
    public void visit(ShowTrace showTrace) {
        appendable.append("SHOW TRACE");
    }

    @Override
    public void visit(ShowDataSources showDatasources) {
        appendable.append("SHOW DATASOURCES");
    }

    @Override
    public void visit(ReloadDataSources clearDataSources) {
        appendable.append("CLEAR DATASOURCES");
    }

    @Override
    public void visit(ReloadFireworks rebuildFireworks) {
        appendable.append("RELOAD FIREWORKS");
    }

    /**
     * reference_definition: REFERENCES tbl_name (index_col_name,...) [MATCH FULL |
     * MATCH PARTIAL | MATCH SIMPLE] [ON DELETE reference_option] [ON UPDATE
     * reference_option] reference_option: RESTRICT | CASCADE | SET NULL | NO ACTION
     */
    @Override
    public void visit(ReferenceDefinition referenceDefinition) {
        appendable.append(" REFERENCES ");
        // tbl_name
        processTableName(referenceDefinition.getTblName());
        // columns
        appendable.append(" (");
        boolean isFst = true;
        for (IndexColumnName indexColumnName : referenceDefinition.getColumns()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(", ");
            }
            indexColumnName.accept(this);
        }
        appendable.append(") ");
        // match
        if (referenceDefinition.getMatchType() != null) {
            switch (referenceDefinition.getMatchType()) {
            case MATCH_FULL:
                appendable.append("MATCH FULL ");
                break;
            case MATCH_PARTIAL:
                appendable.append("MATCH PARTIAL ");
                break;
            case MATCH_SIMPLE:
                appendable.append("MATCH SIMPLE ");
                break;
            }
        }

        if (referenceDefinition.getReferenceOptions() != null) {
            for (ReferenceOption referenceOption : referenceDefinition.getReferenceOptions()) {
                appendable.append(" ");
                referenceOption.accept(this);
            }
        }
    }

    @Override
    public void visit(ReferenceOption referenceOption) {
        // ontype
        if (referenceOption.getOnType() != null) {
            switch (referenceOption.getOnType()) {
            case ON_DELETE:
                appendable.append("ON DELETE ");
                break;
            case ON_UPDATE:
                appendable.append("ON UPDATE ");
                break;
            }

            switch (referenceOption.getReferenceOptionType()) {
            case RESTRICT:
                appendable.append("RESTRICT");
                break;
            case CASCADE:
                appendable.append("CASCADE");
                break;
            case SET_NULL:
                appendable.append("SET NULL");
                break;
            case NO_ACTION:
                appendable.append("NO ACTION");
                break;
            }
        }
    }

    public void visit(IndexDefinition.IndexType indexType) {
        /* USING {BTREE | HASH } */
        appendable.append("USING ");
        if (indexType == IndexDefinition.IndexType.BTREE) {
            appendable.append("BTREE ");
        } else if (indexType == IndexDefinition.IndexType.HASH) {
            appendable.append("HASH ");
        }
    }

    @Override
    public void visit(Algorithm algorithm) {
        /* ALGORITHM [=] {DEFAULT|INPLACE|COPY} */
        appendable.append("ALGORITHM = ");
        switch (algorithm.getAlgorithmType()) {
        case DEFAULT:
            appendable.append("DEFAULT ");
            break;
        case INPLACE:
            appendable.append("INPLACE ");
            break;
        case COPY:
            appendable.append("COPY ");
            break;
        }
    }

    @Override
    public void visit(LockOperation lockOperation) {
        /* LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE} */
        appendable.append("LOCK = ");
        switch (lockOperation.getLockType()) {
        case DEFAULT:
            appendable.append("DEFAULT ");
            break;
        case NONE:
            appendable.append("NONE ");
            break;
        case SHARED:
            appendable.append("SHARED ");
            break;
        case EXCLUSIVE:
            appendable.append("EXCLUSIVE ");
            break;
        }
    }

    @Override
    public void visit(EnableKeys enableKeys) {
        switch (enableKeys.getEnableType()) {
        case ENABLE:
            appendable.append("ENABLE KEYS ");
            break;
        case DISABLE:
            appendable.append("DISABLE KEYS ");
            break;
        }
    }

    @Override
    public void visit(ImportTablespace importTablespace) {
        switch (importTablespace.getImportTSType()) {
        case IMPORT:
            appendable.append("IMPORT TABLESPACE ");
            break;
        case DISCARD:
            appendable.append("DISCARD TABLESPACE ");
            break;
        }
    }

    @Override
    public void visit(ForceOperation foreceOperation) {
        appendable.append("FORCE ");
    }

    @Override
    public void visit(RenameOperation renameOperation) {
        appendable.append("RENAME ");
        if (renameOperation.getRenameType() != null) {
            switch (renameOperation.getRenameType()) {
            case AS:
                appendable.append("AS ");
                break;
            case TO:
                appendable.append("TO ");
                break;
            }
        }

        if (renameOperation.getNewTblName() != null) {
            renameOperation.getNewTblName().accept(this);
        }
    }

    @Override
    public void visit(ConvertCharset convertCharset) {
        /* CONVERT TO CHARACTER SET charset_name [COLLATE collation_name] */
        appendable.append("CONVERT TO CHARACTER SET ");
        if (convertCharset.getCharsetdef() != null && convertCharset.getCharsetdef().getKey() != null) {
            convertCharset.getCharsetdef().getKey().accept(this);
            appendable.append(" ");

            if (convertCharset.getCharsetdef().getValue() != null) {
                appendable.append("COLLATE ");
                convertCharset.getCharsetdef().getValue().accept(this);
            }
        }
    }

    @Override
    public void visit(CharacterSet characterSet) {
        /*
         * [DEFAULT] CHARACTER SET [=] charset_name [COLLATE [=] collation_name]
         */
        if (characterSet.isIsdefault()) {
            appendable.append("DEFAULT ");
        }

        appendable.append("CHARACTER SET = ");
        if (characterSet.getCharsetdef() != null && characterSet.getCharsetdef().getKey() != null) {
            characterSet.getCharsetdef().getKey().accept(this);
            appendable.append(" ");

            if (characterSet.getCharsetdef().getValue() != null) {
                appendable.append("COLLATE = ");
                characterSet.getCharsetdef().getValue().accept(this);
            }
        }

    }

    private void visit(List<Identifier> ids) {
        CommerSpliter commerSpliter = new CommerSpliter(appendable);
        for (Identifier id : ids) {
            commerSpliter.split();
            id.accept(this);
        }
    }

    @Override
    public void visit(Orderby orderby) {
        /* ORDER BY col_name [, col_name] ... */
        appendable.append("ORDER BY ");
        CommerSpliter spliter = new CommerSpliter(appendable);
        for (Identifier colName : orderby.getColNames()) {
            spliter.split();
            appendable.append(' ');
            colName.accept(this);
        }
    }

    @Override
    public void visit(AddPartitition addPartitition) {
        appendable.append("ADD PARTITION (");
        if (addPartitition.getPartitionDefinition() != null) {
            addPartitition.getPartitionDefinition().accept(this);
        }
        appendable.append(") ");
    }

    @Override
    public void visit(DropPartitition dropPartitition) {
        appendable.append("DROP PARTITION ");
        visit(dropPartitition.getPartition_names());
        appendable.append(" ");
    }

    @Override
    public void visit(TruncatePartitition truncatePartitition) {
        appendable.append("TRUNCATE PARTITION ");
        if (truncatePartitition.getTruncatePartitionType() != null) {
            switch (truncatePartitition.getTruncatePartitionType()) {
            case ALL:
                appendable.append("ALL");
                break;
            case PARTITION_NAMES:
                visit(truncatePartitition.getPartition_names());
                break;
            }
        }
        appendable.append(" ");
    }

    @Override
    public void visit(CoalescePartition coalescePartition) {
        appendable.append("COALESCE PARTITION ");
        if (coalescePartition.getNumber() != null) {
            coalescePartition.getNumber().accept(this);
        }
        appendable.append(" ");
    }

    @Override
    public void visit(ReorganizePartition reorganizePartition) {
        appendable.append("REORGANIZE PARTITION ");
        if (reorganizePartition.getPartition_names() != null) {
            visit(reorganizePartition.getPartition_names());
            appendable.append(" ");
        }
        appendable.append(" INTO (");
        if (reorganizePartition.getPartitionDefinitionList() != null) {
            CommerSpliter pdSpliter = new CommerSpliter(appendable);
            for (PartitionDefinition pd : reorganizePartition.getPartitionDefinitionList()) {
                pdSpliter.split();
                pd.accept(this);
            }
        }
        appendable.append(") ");
    }

    @Override
    public void visit(ExchangePartition exchangePartition) {
        appendable.append("EXCHANGE PARTITION ");
        if (exchangePartition.getPartition_name() != null) {
            exchangePartition.getPartition_name().accept(this);
            appendable.append(' ');
        }
        appendable.append("WITH TABLE ");
        if (exchangePartition.getTbl_name() != null) {
            exchangePartition.getTbl_name().accept(this);
        }
        appendable.append(" ");
    }

    @Override
    public void visit(AnalyzePartition analyzePartition) {
        appendable.append("ANALYZE PARTITION ");
        if (analyzePartition.getAnalyzePartitionType() != null) {
            switch (analyzePartition.getAnalyzePartitionType()) {
            case ALL:
                appendable.append("ALL");
                break;
            case PARTITION_NAMES:
                visit(analyzePartition.getPartition_names());
                break;
            }
        }
        appendable.append(' ');
    }

    @Override
    public void visit(CheckPartition checkPartition) {
        appendable.append("CHECK PARTITION ");
        if (checkPartition.getCheckPartitionType() != null) {
            switch (checkPartition.getCheckPartitionType()) {
            case ALL:
                appendable.append("ALL");
                break;
            case PARTITION_NAMES:
                visit(checkPartition.getPartition_names());
                break;
            }
        }
        appendable.append(' ');
    }

    @Override
    public void visit(OptimizePartition optimizePartition) {
        appendable.append("OPTIMIZE PARTITION ");
        if (optimizePartition.getOptimizePartitionType() != null) {
            switch (optimizePartition.getOptimizePartitionType()) {
            case ALL:
                appendable.append("ALL");
                break;
            case PARTITION_NAMES:
                visit(optimizePartition.getPartition_names());
                break;
            }
        }
        appendable.append(' ');
    }

    @Override
    public void visit(RebuildPartition rebuildPartition) {
        appendable.append("REBUILD PARTITION ");
        if (rebuildPartition.getRebuildPartitionType() != null) {
            switch (rebuildPartition.getRebuildPartitionType()) {
            case ALL:
                appendable.append("ALL");
                break;
            case PARTITION_NAMES:
                visit(rebuildPartition.getPartition_names());
                break;
            }
        }
        appendable.append(' ');
    }

    @Override
    public void visit(RepairPartition repairPartition) {
        appendable.append("REPAIR PARTITION ");
        if (repairPartition.getRepairPartitionType() != null) {
            switch (repairPartition.getRepairPartitionType()) {
            case ALL:
                appendable.append("ALL");
                break;
            case PARTITION_NAMES:
                visit(repairPartition.getPartition_names());
                break;
            }
        }
        appendable.append(' ');
    }

    @Override
    public void visit(RemovePartitioning removePartitioning) {
        appendable.append("REMOVE PARTITIONING ");
    }

    protected void processTableName(Identifier identifier) {
        identifier.accept(this);
    }

    private static String escape(String str, char findChar, char leadChar) {
        int find = str.indexOf(findChar);
        if (find < 0) {
            return str;
        }
        StringBuilder builder = new StringBuilder(str.length() + 8);
        int index = 0;
        do {
            builder.append(str.substring(index, find));
            builder.append(leadChar);
            index = find;
            find = str.indexOf(findChar, find + 1);
        } while (find >= 0);

        builder.append(str.substring(index));
        return builder.toString();
    }

    public static String convertKeywords(String name) {
        /**
         * <pre>
         * 以下几中情况:
         * 0. 字符串 "name" 或 'name' -> 原样
         * 1. 普通的一个name -> 包上 `
         * 2. 加了`的`name` -> 不变
         * 3. 包含`的特殊情况 ```
         *      3.1 符合输入规范的```, `````, ``````` -> 不变
         *      3.2 转义后的 `, ``, ``` -> 转义后包上` 变为 ```, ````` , ``````` 约定这里不允许包含
         *          3.2的情况,意味着不允许actualTableName等获取到的表名列名调用这个 走到这里都是从sql输入的
         * </pre>
         */
        if (name == null || name.isEmpty()) {
            return name;
        }

        if ("*".equals(name)) {
            return name;
        }

        char ch = name.charAt(0);
        if (ch == '`' || ch == '\'' || ch == '\"') {
            return name;
        }

        // 非`开头, 先转义
        name = escape(name, '`', '`');
        StringBuilder sb = new StringBuilder(2 + name.length());
        sb.append('`');
        sb.append(name);
        sb.append('`');
        return sb.toString();
    }

    @Override
    public void visit(Kill kill) {
        appendable.append("KILL '" + kill.getProcessId() + "'");
    }

    @Override
    public void visit(ReleaseDbLock releaseDbLock) {
        appendable.append("RELEASE ").append("DBLOCK");
    }

    @Override
    public void visit(LockTablesStatement lockTablesStatement) {
        appendable.append("LOCK TABLES ");

        boolean first = true;

        for (LockReference lock : lockTablesStatement.getLocks()) {
            if (first) {
                first = false;
            } else {
                appendable.append(", ");
            }

            lock.getTable().accept(this);
            appendable.append(" ");
            if (lock.getAlias() != null) {
                appendable.append("AS ");
                lock.getAlias().accept(this);
                appendable.append(" ");
            }

            switch (lock.getLockType()) {
            case READ:
                appendable.append("READ");
                break;
            case READ_LOCAL:
                appendable.append("READ LOCAL");
                break;

            case LOW_PRIORITY_WRITE:
                appendable.append("LOW_PRIORITY WRITE");
                break;
            case WRITE:
                appendable.append("WRITE");
                break;
            }
        }

    }

    @Override
    public void visit(UnLockTablesStatement unLockTablesStatement) {
        appendable.append("UNLOCK TABLES");
    }

    @Override
    public void visit(CheckTableStatement node) {

    }

    @Override
    public void visit(ChangeRuleVersionStatement changeRuleVersionStmt) {
        String command = changeRuleVersionStmt.isUpgrading() ? "UPGRADE" : "DOWNGRADE";
        appendable.append(command).append(" RULE VERSION TO ");
        appendable.append(changeRuleVersionStmt.getVersion().longValue());
    }

    @Override
    public void visit(InspectRuleVersionStatement inspectRuleVersionStmt) {
        appendable.append("INSPECT RULE VERSION");
        if (inspectRuleVersionStmt.isIgnoreManager()) {
            appendable.append(" IGNORE MANAGER");
        }
    }

    @Override
    public void visit(ResyncLocalRulesStatement resyncLocalRulesStatement) {
        appendable.append("RESYNC LOCAL RULES");
    }

    @Override
    public void visit(ClearSeqCacheStatement clearSeqCacheStatement) {
        appendable.append("CLEAR SEQUENCE CACHE FOR ");
        if (clearSeqCacheStatement.isAll()) {
            appendable.append("ALL");
        } else {
            appendable.append(clearSeqCacheStatement.getName());
        }
    }

    @Override
    public void visit(InspectGroupSeqRangeStatement inspectGroupSeqRangeStatement) {
        appendable.append("INSPECT GROUP SEQUENCE RANGE FOR ");
        appendable.append(inspectGroupSeqRangeStatement.getName());
    }

    @Override
    public void visit(ShowDbStatus node) {
        appendable.append("SHOW DATABASE STATUS");
        printLikeOrWhere(node.getPattern(), node.getWhere());

        if (node.getLimit() != null) {
            node.getLimit().accept(this);
        }
    }

    @Override
    public void visit(ShowCluster node) {
        appendable.append("SHOW CLUSTER");
    }

    @Override
    public void visit(ShowRO node) {
        appendable.append("SHOW RO");
    }

    @Override
    public void visit(ShowRW node) {
        appendable.append("SHOW RW");
    }

    @Override
    public void visit(ShowProperties node) {
        appendable.append("SHOW PROPERTIES");
    }

    public void visit(ShowReactor node) {
        appendable.append("SHOW REACTOR");
    }

    public void visit(ShowFrontend node) {
        appendable.append(node.isFull() ? "SHOW FULL FRONTEND" : "SHOW FRONTEND");
    }

    public void visit(ShowBackend node) {
        appendable.append(node.isFull() ? "SHOW FULL BACKEND" : "SHOW BACKEND");
    }

    @Override
    public void visit(ShowSlow node) {
        appendable.append("SHOW ");
        if (node.isPhysical()) {
            appendable.append("PHYSICAL_SLOW");
        } else {
            appendable.append("SLOW");
        }

        Expression where = node.getWhere();
        if (where != null) {
            appendable.append(' ');
            where.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    @Override
    public void visit(ShowStats node) {
        appendable.append("SHOW ");
        if (node.isFull()) {
            appendable.append("FULL ");
        }

        appendable.append("STATS");

    }

    @Override
    public void visit(JsonExtractExpression node) {
        node.getLeftOprand().accept(this);

        appendable.append(node.getOperator());
        node.getRightOprand().accept(this);

    }

    @Override
    public void visit(ShowHtc node) {
        appendable.append("SHOW ").append(" HTC");
    }

    @Override
    public void visit(ShowDS showDs) {
        appendable.append("SHOW ").append(" DS");
    }

    @Override
    public void visit(TMSOptimizeTableStatement node) {
        appendable.append("OPTIMIZE ");

        if (node.getMode() != null) {
            switch (node.getMode()) {
            case NO_WRITE_TO_BINLOG:
                appendable.append("NO_WRITE_TO_BINLOG ");
                break;
            case LOCAL:
                appendable.append("LOCAL ");
                break;
            }
        }

        appendable.append("TABLE ");

        boolean isFst = true;
        for (Identifier arg : node.getTableNames()) {
            if (isFst) {
                isFst = false;
            } else {
                appendable.append(',');
            }
            processTableName(arg);
        }
    }

    @Override
    public void visit(DALAnalyzeTableStatement node) {
        appendable.append("ANALYZE ").append(" TABLE ");
        printList(node.getTableNames());
    }

    @Override
    public void visit(CreateOutlineStatement node) {
        appendable.append("CREATE OUTLINE ").append(node.getName())
            .append(" ON ").append(node.getStmt())
            .append(" TO ").append(node.getTarget());
    }

    @Override
    public void visit(DropOutlineStatement node) {
        appendable.append("DROP OUTLINE ").append(node.getName());
    }

    @Override
    public void visit(ResyncOutlineStatement node) {
        appendable.append("RESYNC OUTLINE ").append(node.getName());
    }

    @Override
    public void visit(DisableOutlineStatement node) {
        appendable.append("DISABLE OUTLINE ").append(node.getName());
    }

    @Override
    public void visit(EnableOutlineStatement node) {
        appendable.append("ENABLE OUTLINE ").append(node.getName());
    }

    @Override
    public void visit(ShowOutlines node) {
        appendable.append("SHOW OUTLINES");
    }

    @Override
    public void visit(ShowInstanceType node) {
        appendable.append("SHOW INSTANCE_TYPE");
    }
}
