package com.pythian.hive.udf

import play.api.libs.json._
import play.api.libs.functional.syntax._

import org.apache.hadoop.hive.ql.exec.UDF
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import java.io.IOException
import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector


object JsonUtils {

  def getJsonString(j: JsValue): String = j match {
    case JsString(_) => j.asOpt[String].get
    case JsObject(_) => j.toString()
    case _ => throw new UDFArgumentException("Usage : json_split(jsonString) ")
  }

  def processJsonString(jsonString: String): ArrayList[Array[Any]] = {

    val j: JsValue = Json.parse(jsonString)

    // get an Array of Strings that are the json objects
    val optionArray: Option[Array[JsValue]] = j.asOpt[Array[JsValue]]
    val jsonArray: Array[JsValue] = optionArray.get

    // create an ArrayList of Arrays
    var json: ArrayList[Array[Any]] = new ArrayList[Array[Any]]()

    // fill the ArrayList with Arrays containing two items each, the row_id and json_string
    for(i <- 0 until jsonArray.length) {
      json += Array[Any](i, JsonUtils.getJsonString(jsonArray(i)))
    }
    return json
  }
}

class JsonSplitGenericUDF extends GenericUDF {

  var stringInspector: StringObjectInspector = _

  override def initialize(args: Array[ObjectInspector]): ObjectInspector = {

    if(args.length != 1
      || ! args(0).getCategory().equals(Category.PRIMITIVE)
      || args(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Usage : json_split(jsonstring) ")
      }

    stringInspector = args(0).asInstanceOf[StringObjectInspector]

    var outputColumns = new ArrayList[String]()
    outputColumns.add("row_add")
    outputColumns.add("json_string")

    var outputTypes = new ArrayList[ObjectInspector]()
    outputTypes.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector)
    outputTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    return ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardStructObjectInspector(outputColumns, outputTypes))
  }

  override def evaluate(args: Array[DeferredObject]): AnyRef = {

    try {
      val jsonString: String = stringInspector.getPrimitiveJavaObject(args(0).get())
      return splitJsonString(jsonString)
    } catch {
      case e: IOException => throw new HiveException(e)
      case e: NullPointerException => return null
    }
  }

  override def getDisplayString(args: Array[String]): String = {
    return "json_split(" + args(0) + ")"
  }

  def splitJsonString(jsonString: String): ArrayList[Array[Any]] = {
    return JsonUtils.processJsonString(jsonString)
  }
}
