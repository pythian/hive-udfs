
import java.util.HashMap

import com.pythian.hive.udf.JsonMapGenericUDF
import org.scalatest._
import play.api.libs.json.{Json, JsValue}

class JsonMapGenericUDFSpec extends FlatSpec with Matchers {

  "JsonMapGenericUDF" should "unpack a simple json object" in {

    val udf = new JsonMapGenericUDF
    val map: HashMap[String, String] = new HashMap[String, String](){{ put("x", "1")}; { put("y", "2")}; { put("z", "3")}; }

    val jString: String = "{\"x\":1, \"y\":2, \"z\":\"3\"}"
    val jValue: JsValue = Json.parse(jString)

    udf.unpackJsonObject(jValue) should be (map)

  }
}
