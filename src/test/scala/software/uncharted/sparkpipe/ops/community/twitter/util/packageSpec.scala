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

package software.uncharted.sparkpipe.ops.community.twitter.util

import software.uncharted.sparkpipe.ops
import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType, ArrayType, DoubleType}
import org.scalatest._

class PackageSpec extends FunSpec {
  describe("ops.community.twitter.util") {

    val path = "src/test/resources/sample-users.json"
    val format = "json"

    describe("#addFields()") {
      it("should add a sequence of StructFields to a StructType") {
        val one = StructField("one", BooleanType, true)
        val two = StructField("two",StringType,true)
        val three = StructField("three",StructType(Seq(StructField("a", LongType, true))),true)

        val SCHEMA:StructType = StructType(Seq(one))
        val toAdd = Seq(two, three)

        val UPDATED_SCHEMA = ops.community.twitter.util.addFields(SCHEMA, toAdd)
        assert((UPDATED_SCHEMA.size).equals(3))
        assert(UPDATED_SCHEMA.contains(two))
        assert(UPDATED_SCHEMA.contains(three))
      }
    }
  }
}
