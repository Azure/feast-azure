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

case class SparkBigtableConfig(
    namespace: String,
    projectName: String,
    entityColumns: Array[String],
    timestampColumn: String,
    maxAge: Long
)
object SparkBigtableConfig {
  val NAMESPACE      = "namespace"
  val ENTITY_COLUMNS = "entity_columns"
  val TS_COLUMN      = "timestamp_column"
  val PROJECT_NAME   = "project_name"
  val MAX_AGE        = "max_age"

  def parse(parameters: Map[String, String]): SparkBigtableConfig =
    SparkBigtableConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      projectName = parameters.getOrElse(PROJECT_NAME, "default"),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp"),
      maxAge = parameters.get(MAX_AGE).map(_.toLong).getOrElse(0)
    )
}
