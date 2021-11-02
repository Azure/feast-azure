/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.ingestion

import feast.ingestion.metrics.IngestionPipelineMetrics
import feast.ingestion.sources.bq.BigQueryReader
import feast.ingestion.sources.file.FileReader
import feast.ingestion.validation.{RowValidator, TypeCheck}
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Encoder, Row, SaveMode, SparkSession}

/**
  * Batch Ingestion Flow:
  * 1. Read from source (BQ | File)
  * 2. Map source columns to FeatureTable's schema
  * 3. Validate
  * 4. Store valid rows in redis
  * 5. Store invalid rows in parquet format at `deadletter` destination
  */
object BatchPipeline extends BasePipeline {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    val featureTable = config.featureTable
    val projection =
      BasePipeline.inputProjection(config.source, featureTable.features, featureTable.entities)
    val rowValidator = new RowValidator(featureTable, config.source.eventTimestampColumn)
    val metrics      = new IngestionPipelineMetrics

    val input = config.source match {
      case source: BQSource =>
        BigQueryReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
      case source: FileSource =>
        FileReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
    }

    val projected = if (config.deadLetterPath.nonEmpty) {
      input.select(projection: _*).cache()
    } else {
      input.select(projection: _*)
    }

    TypeCheck.allTypesMatch(projected.schema, featureTable) match {
      case Some(error) =>
        throw new RuntimeException(s"Dataframe columns don't match expected feature types: $error")
      case _ => ()
    }

    implicit val rowEncoder: Encoder[Row] = RowEncoder(projected.schema)

    val validRows = projected
      .map(metrics.incrementRead)
      .filter(rowValidator.allChecks)

    validRows.write
      .format(config.store match {
        case _: RedisConfig     => "feast.ingestion.stores.redis"
        case _: BigTableConfig  => "feast.ingestion.stores.bigtable"
        case _: CassandraConfig => "feast.ingestion.stores.cassandra"
      })
      .option("entity_columns", featureTable.entities.map(_.name).mkString(","))
      .option("namespace", featureTable.name)
      .option("project_name", featureTable.project)
      .option("timestamp_column", config.source.eventTimestampColumn)
      .option("max_age", config.featureTable.maxAge.getOrElse(0L))
      .save()

    config.deadLetterPath foreach { path =>
      projected
        .filter(!rowValidator.allChecks)
        .map(metrics.incrementDeadLetters)
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .save(StringUtils.stripEnd(path, "/") + "/" + SparkEnv.get.conf.getAppId)
    }

    None
  }
}
