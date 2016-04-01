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

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType, ArrayType, DoubleType, IntegerType}
import scala.collection.mutable.{WrappedArray, ArrayBuffer}
import software.uncharted.sparkpipe.ops

/**
* This package contains twitter pipeline operations for Spark
* See the README or {@link software.uncharted.sparkpipe.Pipe} for more information.
*/
package object tweets {

  // scalastyle:off  line.size.limit multiple.string.literals
  val TWEET_SCHEMA_API_INFFERED = StructType(Seq( // inffereed from example
    StructField("contributors",StringType,true),
    StructField("coordinates",StringType,true),
    StructField("created_at",StringType,true),
    StructField("entities",StructType(Seq(
      StructField("hashtags",ArrayType(StructType(Seq(
        StructField("indices",ArrayType(LongType,true),true),
        StructField("text",StringType,true)
      )),true),true),
      StructField("urls",ArrayType(StringType,true),true),
      StructField("user_mentions",ArrayType(StringType,true),true)
    )),true),
    StructField("favorited",BooleanType,true),
    StructField("geo",StringType,true),
    StructField("id",LongType,true),
    StructField("id_str",StringType,true),
    StructField("in_reply_to_screen_name",StringType,true),
    StructField("in_reply_to_status_id",StringType,true),
    StructField("in_reply_to_status_id_str",StringType,true),
    StructField("in_reply_to_user_id",StringType,true),
    StructField("in_reply_to_user_id_str",StringType,true),
    StructField("metadata",StructType(Seq(
      StructField("iso_language_code",StringType,true),
      StructField("result_type",StringType,true)
    )),true),
    StructField("place",StringType,true),
    StructField("retweet_count",LongType,true),
    StructField("retweeted",BooleanType,true),
    StructField("source",StringType,true),
    StructField("text",StringType,true),
    StructField("truncated",BooleanType,true),
    StructField("user",StructType(Seq(StructField("contributors_enabled",BooleanType,true), StructField("created_at",StringType,true), StructField("default_profile",BooleanType,true), StructField("default_profile_image",BooleanType,true), StructField("description",StringType,true), StructField("entities",StructType(Seq(StructField("description",StructType(Seq(StructField("urls",ArrayType(StringType,true),true))),true), StructField("url",StructType(Seq(StructField("urls",ArrayType(StructType(Seq(StructField("expanded_url",StringType,true), StructField("indices",ArrayType(LongType,true),true), StructField("url",StringType,true))),true),true))),true))),true), StructField("favourites_count",LongType,true), StructField("follow_request_sent",StringType,true), StructField("followers_count",LongType,true), StructField("following",StringType,true), StructField("friends_count",LongType,true), StructField("geo_enabled",BooleanType,true), StructField("id",LongType,true), StructField("id_str",StringType,true), StructField("is_translator",BooleanType,true), StructField("lang",StringType,true), StructField("listed_count",LongType,true), StructField("location",StringType,true), StructField("name",StringType,true), StructField("notifications",StringType,true), StructField("profile_background_color",StringType,true), StructField("profile_background_image_url",StringType,true), StructField("profile_background_image_url_https",StringType,true), StructField("profile_background_tile",BooleanType,true), StructField("profile_image_url",StringType,true), StructField("profile_image_url_https",StringType,true), StructField("profile_link_color",StringType,true), StructField("profile_sidebar_border_color",StringType,true), StructField("profile_sidebar_fill_color",StringType,true), StructField("profile_text_color",StringType,true), StructField("profile_use_background_image",BooleanType,true), StructField("protected",BooleanType,true), StructField("screen_name",StringType,true), StructField("show_all_inline_media",BooleanType,true), StructField("statuses_count",LongType,true), StructField("time_zone",StringType,true), StructField("url",StringType,true), StructField("utc_offset",LongType,true), StructField("verified",BooleanType,true))),true)
  ))
  // scalastyle:on

  // XXX the metadata field is missing?
  // scalastyle:off  multiple.string.literals
  val TWEET_SCHEMA:StructType = StructType(Seq( // built manually
    StructField("contributors", StringType, true), // StringType in sample tweet, ArrayType(CONTRIBUTER_SCHEMA) in docs
    StructField("coordinates", StringType, true), // StringType in sample tweet, COORDINATE_SCHEMA in docs
    StructField("created_at", StringType, true),
    StructField("current_user_retweet", StructType(Seq(
      StructField("id", LongType, true),
      StructField("id_str", StringType, true)
    )), true),
    StructField("entities", entities.ENTITY_SCHEMA, true),
    StructField("favorite_count", LongType, true),
    StructField("favorited", BooleanType, true),
    StructField("filter_level", StringType, true),
    StructField("geo", StringType, true), // Depricated, uses coordinate instead. Using StringType as placeholder
    StructField("id", LongType, true),
    StructField("id_str", StringType, true),
    StructField("in_reply_to_screen_name", StringType, true),
    StructField("in_reply_to_status_id", LongType, true),
    StructField("in_reply_to_status_id_str", StringType, true),
    StructField("in_reply_to_user_id", LongType, true),
    StructField("in_reply_to_user_id_str", StringType, true),
    StructField("is_quote_status",BooleanType,true), // Not in docs. but in tweet
    StructField("lang", StringType, true),
    StructField("metadata",StructType(Seq( // Not in docs, but in tweet
      StructField("iso_language_code",StringType,true),
      StructField("result_type",StringType,true)
    )),true),
    StructField("place", places.PLACE_SCHEMA, true),
    StructField("possibly_sensitive", BooleanType, true),
    StructField("quoted_status_id", entities.SIZE_SCHEMA, true),
    StructField("quoted_status_id_str", StringType, true),
    StructField("quoted_status", TWEET_SCHEMA, true),
    StructField("scopes", StructType(Seq(
      StructField("followers", BooleanType, true)
    )), true),
    StructField("retweet_count", LongType, true),
    StructField("retweeted", BooleanType, true),
    StructField("retweeted_status", TWEET_SCHEMA, true),
    StructField("source", StringType, true),
    StructField("text", StringType, true),
    StructField("truncated", BooleanType, true),
    StructField("user", users.USER_SCHEMA, true),
    StructField("withheld_copyright", BooleanType, true),
    StructField("withheld_in_countries", ArrayType(StringType, true), true),
    StructField("withheld_scope", StringType, true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val CONTRIBUTER_SCHEMA = StructType(Seq(
    StructField("id", entities.SIZE_SCHEMA, false),
    StructField("id_str", StringType, true),
    StructField("screen_name", StringType, true)
  ))
  // scalastyle:on

  // scalastyle:off  multiple.string.literals
  val COORDINATE_SCHEMA = StructType(Seq(
    StructField("coordinates", ArrayType(DoubleType, true), true), // collection of float
    StructField("type", StringType, true)
  ))
  // scalastyle:on

  // val ANNOTATION_SCHEMA = None // docs: unused. future/beta home for status annotations.

  /**
  * Create a DataFrame from an input data source
  *
  * @param path A format-specific location String for the source data
  * @param format Specifies the input data source format (parquet by default)
  * @param options A Map[String, String] of options
  * @return a DataFrame createad from the specified source
  */
  def read(
    path: String,
    format: String = "parquet",
    options: Map[String, String] = Map[String, String]()
  )(sqlContext: SQLContext): DataFrame = {
    ops.core.dataframe.io.read(path, format, options, TWEET_SCHEMA)(sqlContext)
  }

  /**
  * Create a new column in the given dataframe of hashtags present in the tweet text
  *
  * @param newCol The column into which to put the hashtags
  * @param sourceCol The column from which to get the hashtags
  * @param input Input pipeline data to transform
  * @return the dataframe with a new column containing the hashtags in the tweet
  **/
  def hashtags(newCol: String = "hashtags", sourceCol: String = "entities.hashtags.text")(input: DataFrame): DataFrame = {
    val hashtagExtractor: WrappedArray[String] => WrappedArray[String] = row => {
      row
    }
    ops.core.dataframe.addColumn(newCol, hashtagExtractor, sourceCol)(input)
  }
}
