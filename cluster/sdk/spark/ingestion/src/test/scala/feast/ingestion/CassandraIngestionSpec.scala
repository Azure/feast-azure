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
package feast.ingestion

import com.datastax.spark.connector.cql.CassandraConnector
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import feast.ingestion.helpers.DataHelper.{generateDistinctRows, rowGenerator, storeAsParquet}
import feast.ingestion.helpers.TestRow
import feast.proto.types.ValueProto.ValueType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.SparkConf
import org.joda.time.DateTime
import org.testcontainers.containers.wait.strategy.Wait

import java.sql.Timestamp
import java.time.{Duration, Instant}

class CassandraIngestionSpec extends SparkSpec with ForAllTestContainer {

  override val container = GenericContainer(
    "cassandra:3.11.10",
    exposedPorts = Seq(9042),
    waitStrategy = Wait
      .forListeningPort()
      .withStartupTimeout(Duration.ofSeconds(120))
  )

  val keyspace = "feast"

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .set("spark.cassandra.connection.host", container.host)
    .set("spark.cassandra.connection.port", container.mappedPort(9042).toString)
    .set(
      s"spark.sql.catalog.feast",
      "com.datastax.spark.connector.datasource.CassandraCatalog"
    )
    .set("spark.cassandra.connection.localDC", "datacenter1")
    .set("feast.store.cassandra.keyspace", keyspace)

  trait Scope {

    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        project = "default",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT)
        )
      ),
      startTime = DateTime.parse("2020-08-01"),
      endTime = DateTime.parse("2020-09-01"),
      store = CassandraConfig(
        CassandraConnection(container.host, container.mappedPort(9042)),
        keyspace,
        CassandraWriteProperties(1024, 5)
      )
    )

    val gen = rowGenerator(DateTime.parse("2020-08-01"), DateTime.parse("2020-09-01"))

    def resetDatabase(): Unit = {
      val connector = CassandraConnector(sparkSession.sparkContext.getConf)
      connector.withSessionDo(session => session.execute("DROP KEYSPACE IF EXISTS feast"))
      sparkSession.sql(
        "CREATE DATABASE feast.feast WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')"
      )
    }

  }

  def decodeAvroValue(input: Array[Byte], jsonFormatSchema: String): GenericRecord = {
    val schema      = new Schema.Parser().parse(jsonFormatSchema)
    val reader      = new GenericDatumReader[Any](schema)
    var result: Any = null

    val decoder = DecoderFactory.get().binaryDecoder(input, 0, input.length, null)
    result = reader.read(result, decoder)
    result.asInstanceOf[GenericRecord]
  }

  "Dataset" should "be ingested in cassandra" in new Scope {
    resetDatabase()
    val rows     = generateDistinctRows(gen, 1000, (_: TestRow).customer).filterNot(_.customer.isEmpty)
    val tempPath = storeAsParquet(sparkSession, rows)
    val configWithOfflineSource = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp")
    )

    BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)
    val avroSchema = sparkSession
      .sql(
        "SELECT schema_ref, avro_schema FROM feast.feast.feast_schema_reference"
      )
      .collect()
      .map(row => row.getAs[Array[Byte]](0).toSeq -> new String(row.getAs[Array[Byte]](1)))
      .toMap

    val storedRows = sparkSession
      .sql(
        "SELECT key, test_fs, test_fs__schema_ref, writeTime(test_fs) FROM feast.feast.default__customer"
      )
      .collect()
      .map { row =>
        val record = decodeAvroValue(
          row.getAs[Array[Byte]](1),
          avroSchema(row.getAs[Array[Byte]](2).toSeq)
        )

        TestRow(
          new String(row.getAs[Array[Byte]](0)),
          record.get("feature1").asInstanceOf[Integer],
          record.get("feature2").asInstanceOf[Float],
          Timestamp.from(Instant.ofEpochMilli(row.getLong(3)))
        )
      }

    storedRows should contain allElementsOf rows

  }

  "Cassandra schema update" should "not fail when column exists" in new Scope {
    resetDatabase()
    val rows     = generateDistinctRows(gen, 1, (_: TestRow).customer).filterNot(_.customer.isEmpty)
    val tempPath = storeAsParquet(sparkSession, rows)
    val configWithOfflineSource = config.copy(
      source = FileSource(tempPath, Map.empty, "eventTimestamp")
    )

    for (_ <- 0 to 2) {
      BatchPipeline.createPipeline(sparkSession, configWithOfflineSource)
    }

  }

}
