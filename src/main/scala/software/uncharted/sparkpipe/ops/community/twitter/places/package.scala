/*
 * Copyright 2016 Uncharted Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.uncharted.sparkpipe.ops.community.twitter

import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType, ArrayType, DoubleType}

/**
 *
 */
package object places {

  val PLACE_SCHEMA = StructType(Seq(
    StructField("attributes", ATTRIBUTE_SCHEMA, true),
    StructField("bounding_box", BOUNDING_BOX_SCHEMA, true),
    StructField("country", StringType, true),
    StructField("country_code", StringType, true),
    StructField("full_name", StringType, true),
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("place_type", StringType, true),
    StructField("url", StringType, true)
  ))

  val ATTRIBUTE_SCHEMA = StructType(Seq(
    StructField("street_address", StringType, true),
    StructField("locality", StringType, true),
    StructField("region", StringType, true),
    StructField("iso3", StringType, true),
    StructField("postal_code", StringType, true),
    StructField("phone", StringType, true),
    StructField("twitter", StringType, true),
    StructField("url", StringType, true),
    StructField("app:id", StringType, true)
  ))

  val BOUNDING_BOX_SCHEMA = StructType(Seq(
    StructField("coordinates", ArrayType(ArrayType(ArrayType(DoubleType, true), true), true), true),
    StructField("type", StringType, true)
  ))
}
