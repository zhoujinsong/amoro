/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.analysis.{AlignedRowLevelIcebergCommandCheck, AlignRowLevelCommandAssignments, CheckMergeIntoTableConditions, MergeIntoIcebergTableResolutionCheck, ProcedureArgumentCoercion, ResolveMergeIntoTableReferences, ResolveProcedures, RewriteDeleteFromIcebergTable, RewriteMergeIntoTable, RewriteUpdateTable}
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.{ExtendedDataSourceV2Strategy, ExtendedV2Writes, OptimizeMetadataOnlyDeleteFromIcebergTable, ReplaceRewrittenRowLevelCommand, RowLevelCommandScanRelationPushDown}
import org.apache.spark.sql.execution.dynamicpruning.RowLevelCommandDynamicPruning

import org.apache.amoro.spark.sql.catalyst.analysis
import org.apache.amoro.spark.sql.catalyst.analysis.{MixedFormatAlignRowLevelCommandAssignments, QueryWithConstraintCheck}
import org.apache.amoro.spark.sql.catalyst.analysis.{QueryWithConstraintCheck, ResolveMergeIntoMixedFormatTableReferences, ResolveMixedFormatCommand, RewriteMixedFormatCommand, RewriteMixedFormatMergeIntoTable}
import org.apache.amoro.spark.sql.catalyst.optimize.{OptimizeWriteRule, RewriteAppendMixedFormatTable, RewriteDeleteFromMixedFormatTable, RewriteUpdateMixedFormatTable}
import org.apache.amoro.spark.sql.catalyst.parser.MixedFormatSqlExtensionsParser
import org.apache.amoro.spark.sql.execution

class MixedFormatSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser {
      case (_, parser) => new MixedFormatSqlExtensionsParser(parser)
    }
    // resolve mixed-format command
    extensions.injectResolutionRule { spark => ResolveMixedFormatCommand(spark) }
    extensions.injectResolutionRule { spark => ResolveMergeIntoMixedFormatTableReferences(spark) }
    extensions.injectResolutionRule { _ => MixedFormatAlignRowLevelCommandAssignments }
    extensions.injectResolutionRule { spark => RewriteMixedFormatMergeIntoTable(spark) }

    extensions.injectPostHocResolutionRule(spark => RewriteMixedFormatCommand(spark))

    // mixed-format optimizer rules
    extensions.injectPostHocResolutionRule { spark => QueryWithConstraintCheck(spark) }
    extensions.injectOptimizerRule { spark => RewriteAppendMixedFormatTable(spark) }
    extensions.injectOptimizerRule { spark => RewriteDeleteFromMixedFormatTable(spark) }
    extensions.injectOptimizerRule { spark => RewriteUpdateMixedFormatTable(spark) }

    // iceberg extensions
    extensions.injectResolutionRule { spark => ResolveMergeIntoTableReferences(spark) }
    extensions.injectResolutionRule { _ => CheckMergeIntoTableConditions }
    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }
    extensions.injectResolutionRule { _ => AlignRowLevelCommandAssignments }
    extensions.injectResolutionRule { _ => RewriteDeleteFromIcebergTable }
    extensions.injectResolutionRule { _ => RewriteUpdateTable }
    extensions.injectResolutionRule { _ => RewriteMergeIntoTable }
    extensions.injectCheckRule { _ => MergeIntoIcebergTableResolutionCheck }
    extensions.injectCheckRule { _ => AlignedRowLevelIcebergCommandCheck }

    // optimizer extensions
    extensions.injectOptimizerRule { _ => ExtendedSimplifyConditionalsInPredicate }
    extensions.injectOptimizerRule { _ => ExtendedReplaceNullWithFalseInPredicate }
    // pre-CBO rules run only once and the order of the rules is important
    // - metadata deletes have to be attempted immediately after the operator optimization
    // - dynamic filters should be added before replacing commands with rewrite plans
    // - scans must be planned before building writes
    extensions.injectPreCBORule { _ => OptimizeMetadataOnlyDeleteFromIcebergTable }
    extensions.injectPreCBORule { _ => RowLevelCommandScanRelationPushDown }
    extensions.injectPreCBORule { _ => ExtendedV2Writes }
    extensions.injectPreCBORule { spark => RowLevelCommandDynamicPruning(spark) }
    extensions.injectPreCBORule { _ => ReplaceRewrittenRowLevelCommand }

    // planner extensions
    extensions.injectPlannerStrategy { spark => ExtendedDataSourceV2Strategy(spark) }
    // mixed-format optimizer rules
    extensions.injectPreCBORule(OptimizeWriteRule)

    // mixed-format strategy rules
    extensions.injectPlannerStrategy { spark => execution.ExtendedMixedFormatStrategy(spark) }

    // iceberg sql parser extensions
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }

    // iceberg procedure analyzer extensions
    extensions.injectResolutionRule { spark => ResolveProcedures(spark) }
  }

}
