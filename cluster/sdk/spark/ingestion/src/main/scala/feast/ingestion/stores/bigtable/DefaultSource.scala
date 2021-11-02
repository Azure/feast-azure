/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.ingestion.stores.bigtable

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import com.google.cloud.bigtable.hbase.BigtableConfiguration
import feast.ingestion.stores.serialization.AvroSerializer
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory.{
  BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING,
  BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
  BIGTABLE_BULK_MAX_ROW_KEY_COUNT,
  BIGTABLE_DATA_CHANNEL_COUNT_KEY,
  BIGTABLE_EMULATOR_HOST_KEY,
  MAX_INFLIGHT_RPCS_KEY
}
import org.apache.hadoop.conf.Configuration

class DefaultSource extends CreatableRelationProvider {
  import DefaultSource._

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val bigtableConf = BigtableConfiguration.configure(
      sqlContext.getConf(PROJECT_KEY),
      sqlContext.getConf(INSTANCE_KEY)
    )

    if (sqlContext.getConf("spark.bigtable.emulatorHost", "").nonEmpty) {
      bigtableConf.set(
        BIGTABLE_EMULATOR_HOST_KEY,
        sqlContext.getConf("spark.bigtable.emulatorHost")
      )
    }

    configureBigTableClient(bigtableConf, sqlContext)

    val rel =
      new BigTableSinkRelation(
        sqlContext,
        new AvroSerializer,
        SparkBigtableConfig.parse(parameters),
        bigtableConf
      )
    rel.createTable()
    rel.saveWriteSchema(data)
    rel.insert(data, overwrite = false)
    rel
  }

  private def configureBigTableClient(bigtableConf: Configuration, sqlContext: SQLContext): Unit = {
    val confs = sqlContext.getAllConfs

    confs.get(CHANNEL_COUNT_KEY).foreach(bigtableConf.set(BIGTABLE_DATA_CHANNEL_COUNT_KEY, _))
    confs.get(MAX_ROW_COUNT_KEY).foreach(bigtableConf.set(BIGTABLE_BULK_MAX_ROW_KEY_COUNT, _))
    confs.get(MAX_INFLIGHT_KEY).foreach(bigtableConf.set(MAX_INFLIGHT_RPCS_KEY, _))

    confs
      .get(ENABLE_THROTTLING_KEY)
      .foreach(
        bigtableConf.set(BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, _)
      )
    confs
      .get(THROTTLING_THRESHOLD_MILLIS_KEY)
      .foreach(
        bigtableConf.set(BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS, _)
      )
  }
}

object DefaultSource {
  private val PROJECT_KEY  = "spark.bigtable.projectId"
  private val INSTANCE_KEY = "spark.bigtable.instanceId"

  private val CHANNEL_COUNT_KEY               = "spark.bigtable.channelCount"
  private val ENABLE_THROTTLING_KEY           = "spark.bigtable.enableThrottling"
  private val THROTTLING_THRESHOLD_MILLIS_KEY = "spark.bigtable.throttlingThresholdMs"
  private val MAX_ROW_COUNT_KEY               = "spark.bigtable.maxRowCount"
  private val MAX_INFLIGHT_KEY                = "spark.bigtable.maxInflightRpcs"
}
