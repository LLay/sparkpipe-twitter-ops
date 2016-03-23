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
package object entities {
  val ENTITY_SCHEMA = None
  val EXTENDED_ENTITY_SCHEMA = None
  val HASHTAG_SCHEMA = StructType(Seq(
    StructField("indices", ArrayType(IntegerType,false), true),
    StructField("text", StringType, false)
  ))
  val MEDIA_SCHEMA = StructType(Seq(
    StructField("display_url",	StringType, true),
    StructField("expanded_url",	StringType, true),
    StructField("id",	SIZES_SCHEMA, true), // Docs types this as Int64, which redirects to 'sizes'
    StructField("id_str",	StringType, true),
    StructField("indices",	ArrayType(IntegerType, true), true),
    StructField("media_url",	StringType, true),
    StructField("media_url_https",	StringType, true),
    StructField("sizes",	SIZES_SCHEMA, true),
    StructField("source_status_id",	SIZES_SCHEMA, true), // Docs types this as Int64, which redirects to 'sizes'
    StructField("source_status_id_str",	StringType, true),
    StructField("type",	StringType, true),
    StructField("url",StringType, true)
  ))
  val URL_SCHEMA = None
  val SIZE_SCHEMA = None
  val SIZES_SCHEMA = None
  val USER_MENTION_SCHEMA = None
  val SYMBOL_SCHEMA = None
}
