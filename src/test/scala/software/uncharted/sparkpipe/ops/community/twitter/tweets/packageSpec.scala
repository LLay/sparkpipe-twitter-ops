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

package software.uncharted.sparkpipe.ops.community.twitter.tweets

import software.uncharted.sparkpipe.{Pipe, ops}
import software.uncharted.sparkpipe.ops.community.twitter.{Spark, Schemas}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest._

class PackageSpec extends FunSpec {
  describe("ops.community.twitter.tweets") {

    val path = "src/test/resources/sample-tweets.json"
    val format = "json"

    describe("#read()") {
      it("should pass arguments to the underlying sparkpipe.ops.core.dataframe.io.read() API") {
        val df = ops.community.twitter.tweets.read(path, format)(Spark.sqlContext)
        assert(df.count == 15)
      }
    }

    describe("TWEET_SCHEMA") {
      it("should be a subset of the schema") {
        val pipe = Pipe(Spark.sqlContext).to(ops.core.dataframe.io.read(path, format))
        assert(Schemas.subset(pipe.run.schema, TWEET_SCHEMA))
      }
    }

    describe("#extractHashtags()") {
      it("should create a new column of hashtags present in the tweet text") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(extractHashtags())
        val df = pipe.run
        val actual = df.select("hashtags").collect
        val desired = Array(Seq("WeLoveErdo\u011fan"), Seq(), Seq("TTIP", "TPP"), Seq(), Seq(), Seq(), Seq(), Seq(), Seq("Obama"), Seq(), Seq(), Seq("Obama", "nuclear"), Seq(), Seq(), Seq())

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }

    describe("#extractMentions()") {
      it("should create a new column of user mentions present in the tweet text") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(extractMentions())
        val df = pipe.run
        val actual = df.select("mentions").collect
        val desired = Array(Seq("seZen__333"), Seq("AbdulkerimYakut"), Seq("Matthijs85"), Seq("PoliticaDivan"), Seq(), Seq("TPM"), Seq(), Seq("DragonflyJonez"), Seq(), Seq("steph93065"), Seq("bootymath"), Seq("mattspetalnick", "mattspetalnick", "davidbrunnstrom"), Seq("heavenlyitalian"), Seq(), Seq("AP"))

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }

    describe("#extractURLs()") {
      it("should create a new column of all URLs present in the tweet object") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(extractMentions())
        val df = pipe.run
        val actual = df.select("urls").collect
        val desired = Array(Seq("seZen__333"), Seq("AbdulkerimYakut"), Seq("Matthijs85"), Seq("PoliticaDivan"), Seq(), Seq("TPM"), Seq(), Seq("DragonflyJonez"), Seq(), Seq("steph93065"), Seq("bootymath"), Seq("mattspetalnick", "mattspetalnick", "davidbrunnstrom"), Seq("heavenlyitalian"), Seq(), Seq("AP"))

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }
  }
}
