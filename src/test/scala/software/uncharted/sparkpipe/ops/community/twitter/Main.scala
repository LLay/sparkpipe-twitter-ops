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

import org.scalatest.tools.Runner

import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType, ArrayType, DoubleType}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.IllegalArgumentException

object Spark {
  val conf = new SparkConf().setAppName("sparkpipe-twitter-ops")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
}

object Schemas {

  /**
  * Prints an error message describing which element of a was not an element of b
  *
  * @param a A StructType to compare
  * @param b A StructType to compare
  * @param trace The path from the root to the current recurive poisition. For Error printout
  **/
  def printError(aField:StructField, b:StructType, trace: Array[String]) = {
    val traceString = trace.mkString(".")
    try {
      val bField = b(aField.name) // XXX might fail if aField not in b. use try
      System.err.println(s"In a but different in b: $traceString")
      System.err.println(s"a: $aField")
      System.err.println(s"b: $bField")
    } catch {
      case e:IllegalArgumentException => {
        System.err.println(s"In a but not in b: $traceString")
        System.err.println(s"a: $aField")
      }
    }
  }

  /**
  * Determines if StructType a is a subset of StructType b
  *
  * @param a A StructType to compare
  * @param b A StructType to compare
  * @param trace The path from the root to the current recurive poisition. For Error printout
  * @return Boolean indicating if a was a subset of b
  **/
  def subset(a:StructType, b:StructType, trace:Array[String] = Array()):Boolean = {
    val diff = a.diff(b)
    return diff.foldLeft(true){ (z, field) => // recurse over the fields in this struct
      val newTrace = trace :+ field.name
      if (field.dataType.isInstanceOf[StructType]) { // nested structure. recurse
        z && subset(
          a(field.name).dataType.asInstanceOf[StructType],
          b(field.name).dataType.asInstanceOf[StructType],
          newTrace)
      } else if (field.dataType.isInstanceOf[ArrayType] && field.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]){ // nested structure. recurse
        z && subset(
          a(field.name).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType],
          b(field.name).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType],
          newTrace)
      } else if ( // a has string type, but b has nested structure. See issue #33
        field.dataType.isInstanceOf[StringType] &&
        b.fieldNames.contains(field.name) &&
        (b(field.name).dataType.isInstanceOf[ArrayType] || b(field.name).dataType.isInstanceOf[StructType])) {
          true
      } else {
        printError(field, b, newTrace)
        false // was not nested, branches differed. fail
      }
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    if (sys.env.isDefinedAt("SPARK_JAVA_OPTS") && sys.env("SPARK_JAVA_OPTS").contains("jdwp=transport=dt_socket")) {
      print("Sleeping for 8 seconds. Please connect your debugger now...")
      Thread.sleep(8000)
    }

    val testResult = Runner.run(Array("-o", "-R", "build/classes/test"))
    if (!testResult) {
      System.exit(1)
    }
  }
}
