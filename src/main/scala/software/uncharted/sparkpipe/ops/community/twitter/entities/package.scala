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
  // StructField("sizes",StructType(Seq(
  //   StructField("large",SIZE_SCHEMA,true),
  //   StructField("medium",SIZE_SCHEMA,true),
  //   StructField("small",SIZE_SCHEMA,true),
  //   StructField("thumb",SIZE_SCHEMA,true)
  // )),true),

  // scalastyle:off  multiple.string.literals
  val HASHTAG_SCHEMA = StructType(Seq(
    StructField("indices", ArrayType(LongType,true), true),
    StructField("text", StringType, true)
  ))
  // scalastyle:on


  // scalastyle:off  multiple.string.literals
  val URL_SCHEMA = StructType(Seq(
    StructField("display_url", StringType, true),
    StructField("expanded_url", StringType, true),
    StructField("indices", ArrayType(LongType, true), true),
    StructField("url", StringType, true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val SIZE_SCHEMA = StructType(Seq(
    StructField("h", LongType, true),
    StructField("resize", StringType, true),
    StructField("w", LongType, true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val SIZES_SCHEMA = StructType(Seq(
    StructField("large", SIZE_SCHEMA, true),
    StructField("medium", SIZE_SCHEMA, true),
    StructField("small", SIZE_SCHEMA, true),
    StructField("thumb", SIZE_SCHEMA, true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val USER_MENTION_SCHEMA = StructType(Seq(
    StructField("id", LongType, true), // Docs types this as Int64, which redirects to 'sizes', but tweet inffered it as LongType
    StructField("id_str", StringType, true),
    StructField("indices", ArrayType(LongType, true), true),
    StructField("name", StringType, true),
    StructField("screen_name", StringType, true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val SYMBOL_SCHEMA = StructType(Seq(
    StructField("text", StringType, true),
    StructField("indices", ArrayType(LongType, true), true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val MEDIA_SCHEMA = StructType(Seq(
    StructField("display_url",StringType,true),
    StructField("expanded_url",StringType,true),
    StructField("id",LongType,true),
    StructField("id_str",StringType,true),
    StructField("indices",ArrayType(LongType,true),true),
    StructField("media_url",StringType,true),
    StructField("media_url_https",StringType,true),
    StructField("sizes", SIZES_SCHEMA, true),
    StructField("source_status_id", LongType, true), // Docs types this as Int64, which redirects to 'sizes', but tweet inffered it as LongType
    StructField("source_status_id_str", StringType, true),
    StructField("source_user_id",LongType,true), // Not in docs but in tweet
    StructField("source_user_id_str",StringType,true), // Not in docs but in tweet
    StructField("type",StringType,true),
    StructField("url",StringType,true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val ENTITY_SCHEMA = StructType(Seq(
    StructField("hashtags", HASHTAG_SCHEMA, true),
    StructField("media", ArrayType(MEDIA_SCHEMA, true), true),
    StructField("symbols", ArrayType(StringType,true),true), // Not in docs, but is in tweets
    StructField("urls", ArrayType(URL_SCHEMA, true), true),
    StructField("user_mentions", ArrayType(USER_MENTION_SCHEMA, true), true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val EXTENDED_ENTITY_SCHEMA = StructType(Seq(
    StructField("media",ArrayType(StructType(Seq( // media schema
      StructField("display_url",StringType,true),
      StructField("expanded_url",StringType,true),
      StructField("id",LongType,true),
      StructField("id_str",StringType,true),
      StructField("indices",ArrayType(LongType,true),true),
      StructField("media_url",StringType,true),
      StructField("media_url_https",StringType,true),
      StructField("sizes",SIZES_SCHEMA,true),
      StructField("type",StringType,true),
      StructField("url",StringType,true)
    )),true),true))
  )
  // scalastyle:on

}
