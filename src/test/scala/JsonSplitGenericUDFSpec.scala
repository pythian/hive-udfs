
import java.util.ArrayList

import com.pythian.hive.udf.JsonSplitGenericUDF
import org.scalatest._

/**
input:
["Element 1", "Element 2", "Element 3"]

output:
[{"row_id":0, "json_string":"Element 1"},
{"row_id":1, "json_string":"Element 2"},
{"row_id":2, "json_string":"Element 3"}]
**/

class JsonSplitGenericUDFSpec extends FlatSpec with Matchers {

      "JsonSplitGenericUDF" should " do a basic array test" in {

        val udf = new JsonSplitGenericUDF
        val jsonString: String = "[\"a\",\"b\",\"c\"]"
        val splits: ArrayList[Array[Any]] = udf.splitJsonString(jsonString)

        splits.size should be (3) 

        // test elements are unpacked correctly
        splits.get(0)(1) should be ("a")
        splits.get(1)(1) should be ("b")
        splits.get(2)(1) should be ("c")

        // test indices are assigned for each element
        splits.get(0)(0) should be (0)
        splits.get(1)(0) should be (1)
        splits.get(2)(0) should be (2)
      }

      "JsonSplitGenericUDF" should "unpack nested json array test" in {

        val udf = new JsonSplitGenericUDF
        val jsonString: String = "[{\"a\":1},{\"b\":\"c\"}]"
        val splits: ArrayList[Array[Any]] = udf.splitJsonString(jsonString)
        
        // nested objects are just strings
        splits.get(0)(1) should be ("{\"a\":1}")
        splits.get(1)(1) should be ("{\"b\":\"c\"}")
      }

      "JsonSplitGenericUDF" should "unpack escaped quote strings" in {

        val udf = new JsonSplitGenericUDF
        val jsonString: String = "[\"Hi there, \\\"quotes\\\"\"]"
        val splits: ArrayList[Array[Any]] = udf.splitJsonString(jsonString)

        // escaped quotes are handled
        splits.get(0)(1) should be ("Hi there, \"quotes\"")
      }
}