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
package feast.ingestion.metrics

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.StreamingMetricSource
import org.apache.spark.sql.streaming.StreamingQueryProgress

class StreamingMetrics extends Serializable {

  private val metricSource: Option[StreamingMetricSource] = {
    val metricsSystem = SparkEnv.get.metricsSystem

    metricsSystem.getSourcesByName(StreamingMetricSource.sourceName) match {
      case Seq(head) => Some(head.asInstanceOf[StreamingMetricSource])
      case _         => None
    }
  }

  def updateStreamingProgress(
      progress: StreamingQueryProgress
  ): Unit = {
    metricSource.foreach(_.updateStreamingProgress(progress))
  }

  def updateKafkaTimestamp(timestamp: Long): Unit = {
    metricSource.foreach(_.updateKafkaTimestamp(timestamp))
  }
}

private object StreamingMetricsLock
