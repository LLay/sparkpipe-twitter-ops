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
import software.uncharted.sparkpipe.ops.community.twitter.Spark
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest._

class PackageSpec extends FunSpec {
  describe("ops.community.twitter.tweets") {

    val path = "src/test/resources/sample-tweets.json"
    val format = "json"

    describe("#read()") {
      it("should pass arguments to the underlying sparkpipe.ops.core.dataframe.io.read() API") {
        val df = ops.community.twitter.tweets.read(path, format)(Spark.sqlContext)

        assert(df.schema.equals(TWEET_SCHEMA)) // XXX may be subset of TWEET_SCHEMA
        assert(df.count == 3)
        // XXX Other tests?
      }
    }

    describe("TWEET_SCHEMA") {
      it("should match expected schema") {
        val pipe = Pipe(Spark.sqlContext).to(ops.core.dataframe.io.read(path, format))

        // FIXME Since the inferreded schema can (/will be) a subset of the official schema, this test can(will) fail.
        // Need to make it check that all fields in the inferred schema match ones in the official schema
        assert(pipe.run.schema.equals(TWEET_SCHEMA))
      }
    }

    describe("#hashtags()") {
      it("should create a new column of hashtags present in the tweet text") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(hashtags())
        val df = pipe.run
        val actual = df.select("hashtags").collect
        val desired = Array(Seq("freebandnames"), Seq("FreeBandNames", "SecondHashtag"), Seq("freebandnames"))

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }
  }
}
