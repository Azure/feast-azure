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
package org.apache.spark.metrics.source

import org.apache.spark.metrics.AtomicLongGauge
import org.apache.spark.sql.streaming.StreamingQueryProgress

class StreamingMetricSource extends BaseMetricSource {
  override val sourceName: String = StreamingMetricSource.sourceName

  private val BATCH_DURATION_GAUGE =
    metricRegistry.register(gaugeWithLabels("batch_duration_ms"), new AtomicLongGauge())
  private val PROCESSED_ROWS_PER_SECOND_GAUGE =
    metricRegistry.register(gaugeWithLabels("input_rows_per_second"), new AtomicLongGauge())
  private val INPUT_ROWS_PER_SECOND_GAUGE =
    metricRegistry.register(gaugeWithLabels("processed_rows_per_second"), new AtomicLongGauge())
  private val LAST_CONSUMED_KAFKA_TIMESTAMP_GAUGE =
    metricRegistry.register(gaugeWithLabels("last_consumed_kafka_timestamp"), new AtomicLongGauge())

  def updateStreamingProgress(progress: StreamingQueryProgress): Unit = {
    BATCH_DURATION_GAUGE.value.set(progress.batchDuration)
    INPUT_ROWS_PER_SECOND_GAUGE.value.set(progress.inputRowsPerSecond.toLong)
    PROCESSED_ROWS_PER_SECOND_GAUGE.value.set(progress.processedRowsPerSecond.toLong)
  }

  def updateKafkaTimestamp(timestamp: Long): Unit = {
    LAST_CONSUMED_KAFKA_TIMESTAMP_GAUGE.value.set(timestamp)
  }
}

object StreamingMetricSource {
  val sourceName = "streaming"
}
