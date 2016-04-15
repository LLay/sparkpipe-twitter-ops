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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.functions.{col, udf}

/**
* This package contains twitter pipeline operations for Spark
* See the README or {@link software.uncharted.sparkpipe.Pipe} for more information.
*/
package object tweets {

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

  // scalastyle:off  multiple.string.literals
  val TWEET_SCHEMA_BASE:StructType = StructType(Seq(
    StructField("contributors",StringType,true), // StringType in sample tweet, ArrayType(CONTRIBUTER_SCHEMA) in docs
    StructField("coordinates",StringType,true), // StringType in sample tweet, COORDINATE_SCHEMA in docs
    StructField("created_at",StringType,true),
    StructField("current_user_retweet", StructType(Seq(
      StructField("id", LongType, true),
      StructField("id_str", StringType, true)
    )),true),
    StructField("entities",entities.ENTITY_SCHEMA,true),
    StructField("favorite_count",LongType,true),
    StructField("favorited",BooleanType,true),
    StructField("filter_level", StringType, true),  // Depricated, uses coordinate instead.
    StructField("geo",StringType,true),
    StructField("id",LongType,true),
    StructField("id_str",StringType,true),
    StructField("in_reply_to_screen_name",StringType,true),
    StructField("in_reply_to_status_id",StringType,true),
    StructField("in_reply_to_status_id_str",StringType,true),
    StructField("in_reply_to_user_id",StringType,true),
    StructField("in_reply_to_user_id_str",StringType,true),
    StructField("is_quote_status",BooleanType,true), // Not in docs, but found in tweet
    StructField("lang",StringType,true),
    StructField("metadata",StructType(Seq( // Not in docs, but found in tweet
      StructField("iso_language_code",StringType,true),
      StructField("result_type",StringType,true))
    ),true),
    StructField("place",StringType,true),
    StructField("possibly_sensitive",BooleanType,true),
    StructField("quoted_status_id", entities.SIZE_SCHEMA, true),
    StructField("quoted_status_id_str", StringType, true),
    StructField("scopes", StructType(Seq(
      StructField("followers", BooleanType, true)
    )), true),
    StructField("retweet_count",LongType,true),
    StructField("retweeted",BooleanType,true),
    StructField("source",StringType,true),
    StructField("text",StringType,true),
    StructField("truncated",BooleanType,true),
    StructField("user",users.USER_SCHEMA,true), // tweet.user.entities.description.urls must be string type
    StructField("withheld_copyright", BooleanType, true),
    StructField("withheld_in_countries", ArrayType(StringType, true), true),
    StructField("withheld_scope", StringType, true)
  ))
  // scalastyle:on

  // scalastyle:off multiple.string.literals
  val TWEET_SCHEMA_BASE_WITH_EXENDED_USER:StructType = StructType(TWEET_SCHEMA_BASE
    .filterNot(s => {s.name == "user"}))
    .add("user", users.USER_SCHEMA_WITH_EXTENDED_ENTITY, true)
  // scalastyle:on

  // scalastyle:off multiple.string.literals
  val TWEET_SCHEMA:StructType = StructType(TWEET_SCHEMA_BASE
    .filterNot(s => {s.name == "place" || s.name == "in_reply_to_status_id" || s.name == "in_reply_to_user_id"}))
    .add("place",places.PLACE_SCHEMA,true)
    .add("in_reply_to_status_id",LongType,true)
    .add("in_reply_to_user_id",LongType,true)
    .add("retweeted_status",TWEET_SCHEMA_BASE_WITH_EXENDED_USER,true) // tweet.user.retweeted_status.user.entities.description.urls must be StructType
    .add("quoted_status", TWEET_SCHEMA_BASE, true)
  // scalastyle:on

  /**
  * A dummy function to take and return a row
  *
  * @return a Row that was passed to it
  */
  val columnExtractor: WrappedArray[String] => WrappedArray[String] = row => {row}

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
  def extractHashtags(newCol: String = "hashtags", sourceCol: String = "entities.hashtags.text")(input: DataFrame): DataFrame = {
    ops.core.dataframe.addColumn(newCol, columnExtractor, sourceCol)(input)
  }

  /**
  * Create a new column in the given dataframe of hashtags present in the tweet text
  *
  * @param newCol The column into which to put the user mentions
  * @param sourceCol The column from which to get the user mentions
  * @param input Input pipeline data to transform
  * @return the dataframe with a new column containing the user mentions in the tweet
  **/
  def extractMentions(newCol: String = "mentions", sourceCol: String = "entities.user_mentions.screen_name")(input: DataFrame): DataFrame = {
    ops.core.dataframe.addColumn(newCol, columnExtractor, sourceCol)(input)
  }

  /**
  * Create a new column of all URLs present in the tweet object, not including those in the retweet object
  *
  * @param newCol The column into which to put the urls
  * @param input Input pipeline data to transform
  * @return the dataframe with a new column containing all urls in the tweet
  **/
  // scalastyle:off null method.length
  def extractURLs(newCol: String = "urls")(input: DataFrame): DataFrame = {
    // List of columns to extract from. Not including 17 more present in the retweet object
    val sourceColsString = Array(
      "user.profile_background_image_url",       // string
      "user.profile_background_image_url_https", // string
      "user.profile_banner_url",                 // string
      "user.profile_image_url",                  // string
      "user.profile_image_url_https",            // string
      "user.url"                                 // string
    )

    val sourceColsArray = Array(
      "entities.media.url",                     // array<string>
      "entities.media.display_url",             // array<string>
      "entities.media.expanded_url",            // array<string>
      "entities.media.media_url",               // array<string>
      "entities.media.media_url_https",         // array<string>
      "entities.urls.display_url",              // array<string>
      "entities.urls.expanded_url",             // array<string>
      "entities.urls.url",                      // array<string>
      "user.entities.description.urls",         // array<string>
      "user.entities.url.urls.display_url",     // array<string>
      "user.entities.url.urls.expanded_url",    // array<string>
      "user.entities.url.urls.url"              // array<string>
    )

    // Create extraction function for string columns
    val appenderString = (sourceUrlCol: String, urlCol: WrappedArray[String]) => {
      if (sourceUrlCol != null) {urlCol :+ sourceUrlCol}
      else {urlCol}
    }
    val sqlfuncString = udf(appenderString)

    // Create extraction function for array<string> columns
    val appenderArray = (sourceUrlCol: WrappedArray[String], urlCol: WrappedArray[String]) => {
      if (sourceUrlCol != null) {urlCol.union(sourceUrlCol)}
      else {urlCol}
    }
    val sqlfuncArray = udf(appenderArray)

    // Create empty column of Array[String]
    val coder: () => Array[String] = () => {Array()}
    val sqlfunc = udf(coder)
    var df = input.withColumn("urls", sqlfunc())

    // Add each url source to the new column
    sourceColsArray.foreach(sourceCol => {
      df = df
      .withColumnRenamed("urls", "urlsTemp")
      .withColumn("urls", sqlfuncArray(col(sourceCol), col("urlsTemp")))
      .drop("urlsTemp")
    })

    // Add each url source to the new column
    sourceColsString.foreach(sourceCol => {
      df = df
      .withColumnRenamed("urls", "urlsTemp")
      .withColumn("urls", sqlfuncString(col(sourceCol), col("urlsTemp")))
      .drop("urlsTemp")
    })

    df
  }
  // scalastyle:on
}
