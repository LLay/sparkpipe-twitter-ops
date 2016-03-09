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
package software.uncharted.sparpipe.ops.community.twitter

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType}

import software.uncharted.sparkpipe.ops.core.dataframe.io._

/**
 * This package contains a twitter pipeline operations for Spark
 * See the README or {@link software.uncharted.sparkpipe.Pipe} for more information.
 */
package object users {

  val USER_SCHEMA = StructType(Seq(StructField("contributors_enabled",BooleanType,true), StructField("created_at",StringType,true), StructField("default_profile",BooleanType,true), StructField("default_profile_image",BooleanType,true), StructField("description",StringType,true), StructField("favourites_count",LongType,true), StructField("follow_request_sent",BooleanType,true), StructField("followers_count",LongType,true), StructField("following",BooleanType,true), StructField("friends_count",LongType,true), StructField("geo_enabled",BooleanType,true), StructField("id",LongType,true), StructField("id_str",StringType,true), StructField("is_translator",BooleanType,true), StructField("lang",StringType,true), StructField("listed_count",LongType,true), StructField("location",StringType,true), StructField("name",StringType,true), StructField("notifications",BooleanType,true), StructField("profile_background_color",StringType,true), StructField("profile_background_image_url",StringType,true), StructField("profile_background_image_url_https",StringType,true), StructField("profile_background_tile",BooleanType,true), StructField("profile_image_url",StringType,true), StructField("profile_image_url_https",StringType,true), StructField("profile_link_color",StringType,true), StructField("profile_sidebar_border_color",StringType,true), StructField("profile_sidebar_fill_color",StringType,true), StructField("profile_text_color",StringType,true), StructField("profile_use_background_image",BooleanType,true), StructField("protected",BooleanType,true), StructField("screen_name",StringType,true), StructField("show_all_inline_media",BooleanType,true), StructField("status",StructType(Seq(StructField("contributors",StringType,true), StructField("coordinates",StringType,true), StructField("created_at",StringType,true), StructField("favorited",BooleanType,true), StructField("geo",StringType,true), StructField("id",LongType,true), StructField("id_str",StringType,true), StructField("in_reply_to_screen_name",StringType,true), StructField("in_reply_to_status_id",LongType,true), StructField("in_reply_to_status_id_str",StringType,true), StructField("in_reply_to_user_id",LongType,true), StructField("in_reply_to_user_id_str",StringType,true), StructField("place",StringType,true), StructField("retweet_count",LongType,true), StructField("retweeted",BooleanType,true), StructField("source",StringType,true), StructField("text",StringType,true), StructField("truncated",BooleanType,true))),true), StructField("statuses_count",LongType,true), StructField("time_zone",StringType,true), StructField("url",StringType,true), StructField("utc_offset",LongType,true), StructField("verified",BooleanType,true)))

}