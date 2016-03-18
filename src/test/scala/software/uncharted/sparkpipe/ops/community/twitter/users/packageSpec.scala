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

    describe("USER_SCHEMA") {
      it("should match expected schema") {
        val FILE_PATH = "src/test/resources/sample-user.json"
        val pipe = Pipe(Spark.sqlContext).to(ops.core.dataframe.io.read(path = FILE_PATH, format = "json"))

        assert(pipe.run.schema.equals(USER_SCHEMA))
      }
    }
  }
}
