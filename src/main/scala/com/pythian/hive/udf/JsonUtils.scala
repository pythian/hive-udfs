package com.pythian.hive.udf

import java.util.ArrayList

import play.api.libs.json._

import scala.collection.JavaConversions._


object JsonUtils {

  def getJsonString(j: JsValue): String = j match {
    case JsString(_) => j.asOpt[String].get
    case _ => j.toString()
  }

  def processJsonString(jsonString: String): ArrayList[Array[Any]] = {

    val j: JsValue = Json.parse(jsonString)

    // get an Array of Strings that are the json objects
    val optionArray: Option[Array[JsValue]] = j.asOpt[Array[JsValue]]
    val jsonArray: Array[JsValue] = optionArray.get

    // create an ArrayList of Arrays
    val json: ArrayList[Array[Any]] = new ArrayList[Array[Any]]()

    // fill the ArrayList with Arrays containing two items each, the row_id and json_string
    for(i <- 0 until jsonArray.length) {
      json += Array[Any](i, JsonUtils.getJsonString(jsonArray(i)))
    }
    return json
  }
}
