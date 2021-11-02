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
package feast.ingestion.stores.serialization

import com.google.common.hash.Hashing
import org.apache.spark.sql.Column
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.types.StructType

class AvroSerializer extends Serializer {
  override type SchemaType = String

  def convertSchema(schema: StructType): String = {
    val avroSchema = SchemaConverters.toAvroType(schema)
    avroSchema.toString
  }

  def schemaReference(schema: String): Array[Byte] = {
    Hashing.murmur3_32().hashBytes(schema.getBytes).asBytes()
  }

  def serializeData(schema: String): Column => Column = to_avro(_, schema)
}
