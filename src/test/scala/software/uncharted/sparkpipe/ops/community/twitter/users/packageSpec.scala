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

package software.uncharted.sparkpipe.ops.community.twitter.users

import org.apache.spark.sql.{SQLContext, DataFrame}
import software.uncharted.sparkpipe.{Pipe, ops, Spark}
import org.scalatest._

class PackageSpec extends FunSpec {
  describe("ops.community.twitter.users") {

    val path = "src/test/resources/sample-users.json"
    val format = "json"

    describe("#read()") {
      it("should pass arguments to the underlying sparkpipe.ops.core.dataframe.io.read() API") {
        val df = ops.community.twitter.users.read(path, format)(Spark.sqlContext)

        assert(df.schema.equals(USER_SCHEMA)) // is this a duplicate test of TWEET_SCHEMA?
        assert(df.count == 2)
        // Other tests?
      }
    }

    describe("USER_SCHEMA") {
      it("should match expected schema") {
        val pipe = Pipe(Spark.sqlContext).to(ops.core.dataframe.io.read(path, format))

        assert(pipe.run.schema.equals(USER_SCHEMA))
      }
    }
  }
}
