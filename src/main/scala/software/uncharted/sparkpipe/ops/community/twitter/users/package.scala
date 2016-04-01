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
import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType, ArrayType, DoubleType}
import software.uncharted.sparkpipe.ops

/**
 * This package contains twitter pipeline operations for Spark
 * See the README or {@link software.uncharted.sparkpipe.Pipe} for more information.
 */
package object users {

  // scalastyle:off  line.size.limit multiple.string.literals
  val USER_SCHEMA_OLD = StructType(Seq( // inferred from example user/lookup
    StructField("_corrupt_record",StringType,true),
    StructField("contributors_enabled",BooleanType,true),
    StructField("created_at",StringType,true),
    StructField("default_profile",BooleanType,true),
    StructField("default_profile_image",BooleanType,true),
    StructField("description",StringType,true),
    StructField("favourites_count",LongType,true),
    StructField("follow_request_sent",BooleanType,true),
    StructField("followers_count",LongType,true),
    StructField("following",BooleanType,true),
    StructField("friends_count",LongType,true),
    StructField("geo_enabled",BooleanType,true),
    StructField("id",LongType,true),
    StructField("id_str",StringType,true),
    StructField("is_translator",BooleanType,true),
    StructField("lang",StringType,true),
    StructField("listed_count",LongType,true),
    StructField("location",StringType,true),
    StructField("name",StringType,true),
    StructField("notifications",BooleanType,true),
    StructField("profile_background_color",StringType,true),
    StructField("profile_background_image_url",StringType,true),
    StructField("profile_background_image_url_https",StringType,true),
    StructField("profile_background_tile",BooleanType,true),
    StructField("profile_banner_url",StringType,true),
    StructField("profile_image_url",StringType,true),
    StructField("profile_image_url_https",StringType,true),
    StructField("profile_link_color",StringType,true),
    StructField("profile_sidebar_border_color",StringType,true),
    StructField("profile_sidebar_fill_color",StringType,true),
    StructField("profile_text_color",StringType,true),
    StructField("profile_use_background_image",BooleanType,true),
    StructField("protected",BooleanType,true),
    StructField("screen_name",StringType,true),
    StructField("show_all_inline_media",BooleanType,true),
    StructField("status",StructType(Seq(
      StructField("contributors",StringType,true),
      StructField("coordinates",StringType,true),
      StructField("created_at",StringType,true),
      StructField("favorited",BooleanType,true),
      StructField("geo",StringType,true),
      StructField("id",LongType,true),
      StructField("id_str",StringType,true),
      StructField("in_reply_to_screen_name",StringType,true),
      StructField("in_reply_to_status_id",StringType,true),
      StructField("in_reply_to_status_id_str",StringType,true),
      StructField("in_reply_to_user_id",StringType,true),
      StructField("in_reply_to_user_id_str",StringType,true),
      StructField("place",StringType,true),
      StructField("possibly_sensitive",BooleanType,true),
      StructField("retweet_count",LongType,true),
      StructField("retweeted",BooleanType,true),
      StructField("retweeted_status",StructType(Seq(
        StructField("contributors",ArrayType(LongType,true),true),
        StructField("coordinates",StringType,true),
        StructField("created_at",StringType,true),
        StructField("favorited",BooleanType,true),
        StructField("geo",StringType,true),
        StructField("id",LongType,true),
        StructField("id_str",StringType,true),
        StructField("in_reply_to_screen_name",StringType,true),
        StructField("in_reply_to_status_id",StringType,true),
        StructField("in_reply_to_status_id_str",StringType,true),
        StructField("in_reply_to_user_id",StringType,true),
        StructField("in_reply_to_user_id_str",StringType,true),
        StructField("place",StringType,true),
        StructField("possibly_sensitive",BooleanType,true),
        StructField("retweet_count",LongType,true),
        StructField("retweeted",BooleanType,true),
        StructField("source",StringType,true),
        StructField("text",StringType,true),
        StructField("truncated",BooleanType,true))),true),
      StructField("source",StringType,true),
      StructField("text",StringType,true),
      StructField("truncated",BooleanType,true)
    )),true),
    StructField("statuses_count",LongType,true),
    StructField("time_zone",StringType,true),
    StructField("url",StringType,true),
    StructField("utc_offset",LongType,true),
    StructField("verified",BooleanType,true)
  ))
  // scalastyle:on

  // scalastyle:off  line.size.limit multiple.string.literals
  val USER_SCHEMA_MINE = StructType(Seq( // Built manually
    StructField("contributors_enabled",BooleanType,true),
    StructField("created_at",StringType,true),
    StructField("default_profile",BooleanType,true),
    StructField("default_profile_image",BooleanType,true),
    StructField("description",StringType,true),
    StructField("entities", entities.ENTITY_SCHEMA, true),
    StructField("favourites_count",LongType,true), // Docs: int
    StructField("follow_request_sent",BooleanType,true),
    StructField("followers_count",LongType,true),  // Docs: int
    StructField("following",BooleanType,true),
    StructField("friends_count",LongType,true), // Docs: int
    StructField("geo_enabled",BooleanType,true),
    StructField("has_extended_profile",BooleanType,true), // not in docs. but in tweet
    StructField("id",LongType,true), // Int64
    StructField("id_str",StringType,true),
    StructField("is_translation_enabled",BooleanType,true),// not in docs. but in tweet
    StructField("is_translator",BooleanType,true),
    StructField("lang",StringType,true),
    StructField("listed_count",LongType,true), // Docs: int
    StructField("location",StringType,true),
    StructField("name",StringType,true),
    StructField("notifications",BooleanType,true),
    StructField("profile_background_color",StringType,true),
    StructField("profile_background_image_url",StringType,true),
    StructField("profile_background_image_url_https",StringType,true),
    StructField("profile_background_tile",BooleanType,true),
    StructField("profile_banner_url",StringType,true),
    StructField("profile_image_url",StringType,true),
    StructField("profile_image_url_https",StringType,true),
    StructField("profile_link_color",StringType,true),
    StructField("profile_sidebar_border_color",StringType,true),
    StructField("profile_sidebar_fill_color",StringType,true),
    StructField("profile_text_color",StringType,true),
    StructField("profile_use_background_image",BooleanType,true),
    StructField("protected",BooleanType,true),
    StructField("screen_name",StringType,true),
    StructField("show_all_inline_media",BooleanType,true),
    StructField("status", tweets.TWEET_SCHEMA, true),
    StructField("statuses_count",LongType,true), // Docs: int
    StructField("time_zone",StringType,true),
    StructField("url",StringType,true),
    StructField("utc_offset",LongType,true),
    StructField("verified",BooleanType,true),
    StructField("withheld_in_countries", ArrayType(StringType, true), true),
    StructField("withheld_scope", StringType, true)
  ))

  val USER_SCHEMA = StructType(Seq(
  StructField("contributors_enabled",BooleanType,true),
  StructField("created_at",StringType,true),
  StructField("default_profile",BooleanType,true),
  StructField("default_profile_image",BooleanType,true),
  StructField("description",StringType,true),

  StructField("entities",StructType(Seq(
  StructField("description",StructType(Seq(
  StructField("urls",ArrayType(StringType,true),true))),true),
  StructField("url",StructType(Seq(
  StructField("urls",ArrayType(StructType(Seq(
  StructField("display_url",StringType,true),
  StructField("expanded_url",StringType,true),
  StructField("indices",ArrayType(LongType,true),true),
  StructField("url",StringType,true))),true),true))),true))),true),

  StructField("favourites_count",LongType,true),
  StructField("follow_request_sent",BooleanType,true),
  StructField("followers_count",LongType,true),
  StructField("following",BooleanType,true),
  StructField("friends_count",LongType,true),
  StructField("geo_enabled",BooleanType,true),
  StructField("has_extended_profile",BooleanType,true),
  StructField("id",LongType,true),
  StructField("id_str",StringType,true),
  StructField("is_translation_enabled",BooleanType,true),
  StructField("is_translator",BooleanType,true),
  StructField("lang",StringType,true),
  StructField("listed_count",LongType,true),
  StructField("location",StringType,true),
  StructField("name",StringType,true),
  StructField("notifications",BooleanType,true),
  StructField("profile_background_color",StringType,true),
  StructField("profile_background_image_url",StringType,true),
  StructField("profile_background_image_url_https",StringType,true),
  StructField("profile_background_tile",BooleanType,true),
  StructField("profile_banner_url",StringType,true),
  StructField("profile_image_url",StringType,true),
  StructField("profile_image_url_https",StringType,true),
  StructField("profile_link_color",StringType,true),
  StructField("profile_sidebar_border_color",StringType,true),
  StructField("profile_sidebar_fill_color",StringType,true),
  StructField("profile_text_color",StringType,true),
  StructField("profile_use_background_image",BooleanType,true),
  StructField("protected",BooleanType,true),
  StructField("screen_name",StringType,true),
  StructField("status",StructType(Seq(
  StructField("contributors",StringType,true),
  StructField("coordinates",StringType,true),
  StructField("created_at",StringType,true),
  // StructField("entities", entities.ENTITY_SCHEMA, true),
  StructField("entities",StructType(Seq(
  StructField("hashtags", entities.HASHTAG_SCHEMA, true),
  // StructField("hashtags",ArrayType(StructType(Seq(
  // StructField("indices",ArrayType(LongType,true),true),
  // StructField("text",StringType,true))),true),true),

  // StructField("media", ArrayType(entities.MEDIA_SCHEMA), true),
  StructField("media",ArrayType(StructType(Seq(
  StructField("display_url",StringType,true),
  StructField("expanded_url",StringType,true),
  StructField("id",LongType,true),
  StructField("id_str",StringType,true),
  StructField("indices",ArrayType(LongType,true),true),
  StructField("media_url",StringType,true),
  StructField("media_url_https",StringType,true),
  StructField("sizes", entities.SIZES_SCHEMA, true),
  StructField("type",StringType,true),
  StructField("url",StringType,true))),true),true),

  StructField("symbols",ArrayType(StringType,true),true),
  StructField("urls",ArrayType(StructType(Seq(
  StructField("display_url",StringType,true),
  StructField("expanded_url",StringType,true),
  StructField("indices",ArrayType(LongType,true),true),
  StructField("url",StringType,true))),true),true),
  StructField("user_mentions",ArrayType(StringType,true),true))),true),

  StructField("extended_entities",StructType(Seq(
  StructField("media",ArrayType(StructType(Seq(
  StructField("display_url",StringType,true),
  StructField("expanded_url",StringType,true),
  StructField("id",LongType,true),
  StructField("id_str",StringType,true),
  StructField("indices",ArrayType(LongType,true),true),
  StructField("media_url",StringType,true),
  StructField("media_url_https",StringType,true),
  StructField("sizes",StructType(Seq(
  StructField("large",StructType(Seq(
  StructField("h",LongType,true),
  StructField("resize",StringType,true),
  StructField("w",LongType,true))),true),
  StructField("medium",StructType(Seq(
  StructField("h",LongType,true),
  StructField("resize",StringType,true),
  StructField("w",LongType,true))),true),
  StructField("small",StructType(Seq(
  StructField("h",LongType,true),
  StructField("resize",StringType,true),
  StructField("w",LongType,true))),true),
  StructField("thumb",StructType(Seq(
  StructField("h",LongType,true),
  StructField("resize",StringType,true),
  StructField("w",LongType,true))),true))),true),
  StructField("type",StringType,true),
  StructField("url",StringType,true))),true),true))),true),
  StructField("favorite_count",LongType,true),
  StructField("favorited",BooleanType,true),
  StructField("geo",StringType,true),
  StructField("id",LongType,true),
  StructField("id_str",StringType,true),
  StructField("in_reply_to_screen_name",StringType,true),
  StructField("in_reply_to_status_id",StringType,true),
  StructField("in_reply_to_status_id_str",StringType,true),
  StructField("in_reply_to_user_id",StringType,true),
  StructField("in_reply_to_user_id_str",StringType,true),
  StructField("is_quote_status",BooleanType,true),
  StructField("lang",StringType,true),
  StructField("place",StringType,true),
  StructField("possibly_sensitive",BooleanType,true),
  StructField("retweet_count",LongType,true),
  StructField("retweeted",BooleanType,true),
  StructField("source",StringType,true),
  StructField("text",StringType,true),
  StructField("truncated",BooleanType,true))),true),
  StructField("statuses_count",LongType,true),
  StructField("time_zone",StringType,true),
  StructField("url",StringType,true),
  StructField("utc_offset",LongType,true),
  StructField("verified",BooleanType,true)))

  // scalastyle:on



  /**
  * Create a DataFrame from an input data source ... containing tweets from the twitter rest api]
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
    ops.core.dataframe.io.read(path, format, options, USER_SCHEMA)(sqlContext)
  }
}
