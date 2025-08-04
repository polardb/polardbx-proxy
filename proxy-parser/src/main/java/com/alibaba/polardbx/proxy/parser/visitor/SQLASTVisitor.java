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
 * (created at 2011-5-30)
 */
package com.alibaba.polardbx.proxy.parser.visitor;

import com.alibaba.polardbx.proxy.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.PolyadicOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.UnaryOperatorExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.BetweenAndExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionEqualsExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionIsExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.ComparisionNullSafeEqualsExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.comparison.InExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.logical.LogicalAndExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.logical.LogicalOrExpression;
import com.alibaba.polardbx.proxy.parser.ast.expression.misc.InExpressionList;
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
import com.alibaba.polardbx.proxy.parser.ast.fragment.OrderBy;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.ColumnDefinition;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.TableOptions;
import com.alibaba.polardbx.proxy.parser.ast.fragment.ddl.datatype.DataType;
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

/**
 * @author QIU Shuo
 */
public interface SQLASTVisitor {

    void visit(ComparisionIsExpression node);

    void visit(InExpressionList node);

    void visit(LikeExpression node);

    void visit(CollateExpression node);

    void visit(UserExpression node);

    void visit(UnaryOperatorExpression node);

    void visit(BinaryOperatorExpression node);

    void visit(PolyadicOperatorExpression node);

    void visit(LogicalAndExpression node);

    void visit(LogicalOrExpression node);

    void visit(ComparisionEqualsExpression node);

    void visit(ComparisionNullSafeEqualsExpression node);

    void visit(InExpression node);

    void visit(JsonExtractExpression node);

    // -------------------------------------------------------
    void visit(FunctionExpression node);

    void visit(Char node);

    void visit(Convert node);

    void visit(Trim node);

    void visit(Cast node);

    void visit(Avg node);

    void visit(Max node);

    void visit(Min node);

    void visit(Sum node);

    void visit(Count node);

    void visit(GroupConcat node);

    void visit(Extract node);

    void visit(Timestampdiff node);

    void visit(Timestampadd node);

    void visit(GetFormat node);

    // -------------------------------------------------------
    void visit(IntervalPrimary node);

    void visit(LiteralBitField node);

    void visit(LiteralBoolean node);

    void visit(LiteralHexadecimal node);

    void visit(LiteralNull node);

    void visit(LiteralNumber node);

    void visit(LiteralString node);

    void visit(CaseWhenOperatorExpression node);

    void visit(DefaultValue node);

    void visit(PlaceHolder node);

    void visit(Identifier node);

    void visit(MatchExpression node);

    void visit(ParamMarker node);

    void visit(RowExpression node);

    void visit(SysVarPrimary node);

    void visit(UsrDefVarPrimary node);

    // -------------------------------------------------------
    void visit(IndexHint node);

    void visit(InnerJoin node);

    void visit(NaturalJoin node);

    void visit(OuterJoin node);

    void visit(StraightJoin node);

    void visit(SubqueryFactor node);

    void visit(TableReferences node);

    void visit(TableRefFactor node);

    void visit(Dual dual);

    void visit(GroupBy node);

    void visit(Limit node);

    void visit(OrderBy node);

    void visit(ColumnDefinition node);

    void visit(IndexOption node);

    void visit(IndexColumnName node);

    void visit(TableOptions node);

    void visit(AlterSpecification node);

    void visit(DataType node);

    // -------------------------------------------------------
    void visit(ShowDbLock showDbLock);

    void visit(ShowAuthors node);

    void visit(ShowBinaryLog node);

    void visit(ShowBinLogEvent node);

    void visit(ShowCharaterSet node);

    void visit(ShowCollation node);

    void visit(ShowColumns node);

    void visit(ShowContributors node);

    void visit(ShowCreate node);

    void visit(ShowDatabases node);

    void visit(ShowEngine node);

    void visit(ShowEngines node);

    void visit(ShowErrors node);

    void visit(ShowEvents node);

    void visit(ShowFunctionCode node);

    void visit(ShowFunctionStatus node);

    void visit(ShowGrantsStatement node);

    void visit(ShowIndex node);

    void visit(ShowMasterStatus node);

    void visit(ShowOpenTables node);

    void visit(ShowPlugins node);

    void visit(ShowPrivileges node);

    void visit(ShowProcedureCode node);

    void visit(ShowProcedureStatus node);

    void visit(ShowProcesslist node);

    void visit(ShowProfile node);

    void visit(ShowProfiles node);

    void visit(ShowSlaveHosts node);

    void visit(ShowSlaveStatus node);

    void visit(ShowStatus node);

    void visit(ShowStc node);

    void visit(ShowTables node);

    void visit(ShowTableStatus node);

    void visit(ShowTriggers node);

    void visit(ShowVariables node);

    void visit(ShowWarnings node);

    void visit(DescTableStatement node);

    void visit(DALSetStatement node);

    void visit(DALSetSimpleStatement node);

    void visit(DALSetNamesStatement node);

    void visit(DALSetCharacterSetStatement node);

    void visit(CheckTableStatement node);

    void visit(ChangeRuleVersionStatement node);

    void visit(InspectRuleVersionStatement node);

    void visit(ResyncLocalRulesStatement node);

    void visit(ClearSeqCacheStatement node);

    void visit(InspectGroupSeqRangeStatement node);

    void visit(ShowDbStatus node);

    void visit(ShowCluster node);

    void visit(ShowRO node);

    void visit(ShowRW node);

    void visit(ShowProperties node);

    void visit(ShowReactor node);

    void visit(ShowFrontend node);

    void visit(ShowBackend node);

    // -------------------------------------------------------
    void visit(DMLCallStatement node);

    void visit(DMLDeleteStatement node);

    void visit(DMLInsertStatement node);

    void visit(DMLReplaceStatement node);

    void visit(DMLSelectStatement node);

    void visit(DMLSelectFromUpdateStatement node);

    void visit(DMLSelectUnionStatement node);

    void visit(DMLUpdateStatement node);

    void visit(MTSSetTransactionStatement node);

    void visit(MTSSavepointStatement node);

    void visit(MTSReleaseStatement node);

    void visit(MTSRollbackStatement node);

    void visit(DDLTruncateStatement node);

    void visit(DDLAlterTableStatement node);

    void visit(DDLCreateIndexStatement node);

    void visit(DDLCreateTableStatement node);

    void visit(DDLRenameTableStatement node);

    void visit(DDLDropIndexStatement node);

    void visit(DDLDropTableStatement node);

    void visit(ExtDDLCreatePolicy node);

    void visit(ExtDDLDropPolicy node);

    void visit(BetweenAndExpression betweenAndExpression);

    void visit(ShowTopology showTopology);

    void visit(DMLLoadStatement dmlLoadStatement);

    void visit(ShowPartitions showPartitions);

    void visit(ShowBroadcasts showBroadcasts);

    void visit(ShowRule showRule);

    void visit(ShowRuleStatusStatement showRuleStatus);

    void visit(ShowDdlStatusStatement showDdlStatus);

    void visit(ReloadSchema reloadSchema);

    void visit(ReloadUsers reloadUsers);

    void visit(DALPrepareStatement node);

    void visit(DALExecuteStatement node);

    void visit(DALDeallocateStatement node);

    void visit(ShowTrace showTrace);

    void visit(ShowDataSources showDatasources);

    void visit(ReloadDataSources clearDataSources);
    // -------------------------------------------------------

    void visit(ReloadFireworks rebuildFireworks);

    // -------------------------------------------------------
    void visit(AddColumn addColumn);

    void visit(AddColumns addColumns);

    void visit(AddIndex addIndex);

    void visit(IndexDefinition indexDefinition);

    void visit(AddFullTextIndex addFullTextIndex);

    void visit(AddPrimaryKey addPrimaryKey);

    void visit(AddForeignKey addForeignKey);

    void visit(AddSpatialIndex addSpatialIndex);

    void visit(AddUniqueKey addUniqueKey);

    void visit(AlterColumnDefaultVal alterColumnDefaultVal);

    void visit(ChangeColumn changeColumn);

    void visit(ModifyColumn modifyColumn);

    void visit(DropColumn dropColumn);

    void visit(DropIndex dropIndex);

    void visit(DropPrimaryKey dropPrimaryKey);

    void visit(DropForeignKey foreignKey);

    void visit(ReferenceDefinition referenceDefinition);

    void visit(ReferenceOption referenceOption);

    void visit(Algorithm algorithm);

    void visit(LockOperation lockOperation);

    void visit(EnableKeys enableKeys);

    void visit(ImportTablespace importTablespace);

    void visit(ForceOperation foreceOperation);

    void visit(RenameOperation renameOperation);

    void visit(ConvertCharset convertCharset);

    void visit(CharacterSet characterSet);

    void visit(Orderby orderby);

    void visit(AddPartitition addPartitition);

    void visit(DropPartitition dropPartitition);

    void visit(TruncatePartitition truncatePartitition);

    void visit(CoalescePartition coalescePartition);

    void visit(ReorganizePartition reorganizePartition);

    void visit(ExchangePartition exchangePartition);

    void visit(AnalyzePartition analyzePartition);

    void visit(CheckPartition checkPartition);

    // void visit()

    void visit(OptimizePartition optimizePartition);

    void visit(RebuildPartition rebuildPartition);

    void visit(RepairPartition repairPartition);

    void visit(RemovePartitioning removePartitioning);

    // -------------------------------------------------------
    void visit(DBPartitionOptions DBPartitionOptions);

    void visit(DBPartitionBy DBPartitionBy);

    void visit(TBPartitionBy TBPartitionBy);

    // -------------------------------------------------------
    void visit(SubpartitionDefinition subpartitionDefinition);

    void visit(PartitionDefinition partitionDefinition);

    void visit(PartitionOptions partitionOptions);

    void visit(SubPartitionBy subPartitionBy);

    void visit(PartitionBy partitionBy);

    void visit(Kill kill);

    void visit(ReleaseDbLock releaseDbLock);

    void visit(LockTablesStatement lockTablesStatement);

    void visit(UnLockTablesStatement unLockTablesStatement);

    void visit(ShowSlow showSlow);

    void visit(ShowStats showStats);

    void visit(ShowHtc node);

    void visit(TMSOptimizeTableStatement tmsOptimizeTableStatement);

    void visit(ShowDS showDs);

    void visit(DALAnalyzeTableStatement node);

    void visit(CreateOutlineStatement node);

    void visit(DropOutlineStatement node);

    void visit(ResyncOutlineStatement node);

    void visit(DisableOutlineStatement node);

    void visit(EnableOutlineStatement node);

    void visit(ShowOutlines node);

    void visit(ShowInstanceType node);
}
