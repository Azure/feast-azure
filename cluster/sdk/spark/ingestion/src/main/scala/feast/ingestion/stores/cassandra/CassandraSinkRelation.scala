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
package feast.ingestion.stores.cassandra

import feast.ingestion.utils.StringUtils
import feast.ingestion.stores.serialization.Serializer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext}

import scala.util.{Failure, Success, Try}

class CassandraSinkRelation(
    override val sqlContext: SQLContext,
    val serializer: Serializer,
    val config: SparkCassandraConfig
) extends BaseRelation
    with InsertableRelation
    with Serializable {
  override def schema: StructType = ???

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val featureFields = data.schema.fields
      .filterNot(f => isSystemColumn(f.name))

    val featureColumns = featureFields.map(f => data(f.name))

    val entityColumns   = config.entityColumns.map(c => data(c).cast(StringType))
    val schema          = serializer.convertSchema(StructType(featureFields))
    val schemaReference = serializer.schemaReference(schema)

    val writerWithoutTTL = data
      .select(
        joinEntityKey(struct(entityColumns: _*)).alias("key"),
        serializer.serializeData(schema)(struct(featureColumns: _*)).alias(columnName),
        col(config.timestampColumn).alias("ts")
      )
      .withColumn(schemaRefColumnName, lit(schemaReference))
      .writeTo(fullTableReference)
      .option("writeTime", "ts")

    val writer =
      if (config.maxAge <= 0)
        writerWithoutTTL
      else writerWithoutTTL.option("ttl", config.maxAge.toString)

    writer.append()
  }

  val maxTableNameLength = 48

  def sanitizedForCassandra(expr: String): String = {
    val replacedString = expr.replace('-', '_')
    StringUtils.trimAndHash(replacedString, maxTableNameLength)
  }

  val tableName = {
    val entities = config.entityColumns.mkString("__")
    sanitizedForCassandra(s"${config.projectName}__${entities}")
  }

  val keyspace = sqlContext.sparkContext.getConf.get("feast.store.cassandra.keyspace")

  val sparkCatalog = "feast"

  val fullTableReference = s"${sparkCatalog}.${keyspace}.`${tableName}`"

  val columnName = sanitizedForCassandra(config.namespace)

  val schemaRefColumnName = sanitizedForCassandra(s"${config.namespace}__schema_ref")

  val schemaTableName = s"${sparkCatalog}.${keyspace}.feast_schema_reference"

  def createTable(): Unit = {

    sqlContext.sql(s"""
    |CREATE TABLE IF NOT EXISTS ${fullTableReference}
    |(key BINARY)
    |USING cassandra
    |PARTITIONED BY (key)
    |""".stripMargin)

    Try(sqlContext.sql(s"""
         |ALTER TABLE ${fullTableReference}
         |ADD COLUMNS (${columnName} BINARY, ${schemaRefColumnName} BINARY)
         |""".stripMargin)) match {
      case Success(_) | Failure(_: AnalysisException) =>
      case Failure(e)                                 => throw e
    }

  }

  private def joinEntityKey: UserDefinedFunction = udf { r: Row =>
    ((0 until r.size)).map(r.getString).mkString("#").getBytes
  }

  private def isSystemColumn(name: String) =
    (config.entityColumns ++ Seq(config.timestampColumn)).contains(name)

  def saveWriteSchema(data: DataFrame) = {
    sqlContext.sql(s"""
      |CREATE TABLE IF NOT EXISTS ${schemaTableName}
      |(schema_ref BINARY, avro_schema BINARY)
      |USING cassandra
      |PARTITIONED BY (schema_ref)
      |""".stripMargin)

    val featureFields = data.schema.fields
      .filterNot(f => isSystemColumn(f.name))
    val featureSchema = StructType(featureFields)

    val schema = serializer.convertSchema(featureSchema)
    val key    = serializer.schemaReference(schema)

    import sqlContext.sparkSession.implicits._
    val schemaData =
      List((key, schema.asInstanceOf[String].getBytes)).toDF("schema_ref", "avro_schema")

    schemaData.writeTo(schemaTableName).append()
  }
}
