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

import feast.ingestion.utils.JsonUtils
import org.apache.log4j.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.ext.JavaEnumNameSerializer
import org.json4s.jackson.JsonMethods.{parse => parseJSON}
import org.json4s.ext.JavaEnumNameSerializer
import scala.collection.mutable.ArrayBuffer

object IngestionJob {
  import Modes._
  implicit val modesRead: scopt.Read[Modes.Value] = scopt.Read.reads(Modes withName _.capitalize)
  implicit val formats: Formats = DefaultFormats +
    new JavaEnumNameSerializer[feast.proto.types.ValueProto.ValueType.Enum]() +
    ShortTypeHints(List(classOf[ProtoFormat], classOf[AvroFormat]))

  private val logger = Logger.getLogger(getClass.getCanonicalName)

  val parser = new scopt.OptionParser[IngestionJobConfig]("IngestionJob") {
    // ToDo: read version from Manifest
    head("feastSpark.ingestion.IngestionJob", "0.2.5")

    opt[Modes]("mode")
      .action((x, c) => c.copy(mode = x))
      .required()
      .text("Mode to operate ingestion job (offline or online)")

    opt[String](name = "source")
      .action((x, c) => {
        val json = parseJSON(x)
        JsonUtils
          .mapFieldWithParent(json) {
            case (parent: String, (key: String, v: JValue)) if !parent.equals("field_mapping") =>
              JsonUtils.camelize(key) -> v
            case (_, x) => x
          }
          .extract[Sources] match {
          case Sources(file: Some[FileSource], _, _)   => c.copy(source = file.get)
          case Sources(_, bq: Some[BQSource], _)       => c.copy(source = bq.get)
          case Sources(_, _, kafka: Some[KafkaSource]) => c.copy(source = kafka.get)
        }
      })
      .required()
      .text("""JSON-encoded source object (e.g. {"kafka":{"bootstrapServers":...}}""")

    opt[String](name = "feature-table")
      .action((x, c) => {
        val ft = parseJSON(x).camelizeKeys.extract[FeatureTable]

        c.copy(
          featureTable = ft,
          streamingTriggeringSecs = ft.labels.getOrElse("_streaming_trigger_secs", "0").toInt,
          validationConfig =
            ft.labels.get("_validation").map(parseJSON(_).camelizeKeys.extract[ValidationConfig])
        )
      })
      .required()
      .text("JSON-encoded FeatureTableSpec object")

    opt[String](name = "start")
      .action((x, c) => c.copy(startTime = DateTime.parse(x)))
      .text("Start timestamp for offline ingestion")

    opt[String](name = "end")
      .action((x, c) => c.copy(endTime = DateTime.parse(x)))
      .text("End timestamp for offline ingestion")

    opt[String](name = "ingestion-timespan")
      .action((x, c) => {
        val currentTimeUTC = new DateTime(DateTimeZone.UTC);
        val startTime      = currentTimeUTC.withTimeAtStartOfDay().minusDays(x.toInt - 1)
        val endTime        = currentTimeUTC.withTimeAtStartOfDay().plusDays(1)
        c.copy(startTime = startTime, endTime = endTime)
      })
      .text("Ingestion timespan")

    opt[String](name = "redis")
      .action((x, c) => c.copy(store = parseJSON(x).extract[RedisConfig]))

    opt[String](name = "bigtable")
      .action((x, c) => c.copy(store = parseJSON(x).camelizeKeys.extract[BigTableConfig]))

    opt[String](name = "cassandra")
      .action((x, c) => c.copy(store = parseJSON(x).extract[CassandraConfig]))

    opt[String](name = "statsd")
      .action((x, c) => c.copy(metrics = Some(parseJSON(x).extract[StatsDConfig])))

    opt[String](name = "deadletter-path")
      .action((x, c) => c.copy(deadLetterPath = Some(x)))

    opt[String](name = "stencil-url")
      .action((x, c) => c.copy(stencilURL = Some(x)))

    opt[Unit](name = "drop-invalid")
      .action((_, c) => c.copy(doNotIngestInvalidRows = true))

    opt[String](name = "checkpoint-path")
      .action((x, c) => c.copy(checkpointPath = Some(x)))

    opt[Int](name = "triggering-interval")
      .action((x, c) => c.copy(streamingTriggeringSecs = x))

    opt[Unit](name = "databricks-runtime")
      .action((_, c) => c.copy(isDatabricksRuntime = true))

    opt[String](name = "kafka_sasl_auth")
      .action((x, c) => c.copy(kafkaSASL = Some(x)))
  }

  def main(args: Array[String]): Unit = {
    val args_modified = new Array[String](args.length)
    for ( i <- 0 to (args_modified.length - 1)) {
      // Removing spaces between brackets. This is to workaround this known YARN issue (when running Spark on YARN):
      // https://issues.apache.org/jira/browse/SPARK-17814?focusedCommentId=15567964&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-15567964
      // Also remove the unncessary back slashes
      args_modified(i) = args(i).replace(" }", "}");
      args_modified(i) = args_modified(i).replace("\\", "\\\"");
    }
    println("arguments received:",args_modified.toList)
    parser.parse(args_modified, IngestionJobConfig()) match {
      case Some(config) =>
        println(s"Starting with config $config")
        config.mode match {
          case Modes.Offline =>
            val sparkSession = BasePipeline.createSparkSession(config)
            try {
              BatchPipeline.createPipeline(sparkSession, config)
            } catch {
              case e: Throwable =>
                logger.fatal("Batch ingestion failed", e)
                throw e
            } finally {
              if (!config.isDatabricksRuntime) {
                sparkSession.close()
              }
            }
          case Modes.Online =>
            val sparkSession = BasePipeline.createSparkSession(config)
            try {
              StreamingPipeline.createPipeline(sparkSession, config).get.awaitTermination
            } catch {
              case e: Throwable =>
                logger.fatal("Streaming ingestion failed", e)
                throw e
            } finally {
              if (!config.isDatabricksRuntime) {
                sparkSession.close()
              }
            }
        }
      case None =>
        println("Parameters can't be parsed")
    }
  }

}
