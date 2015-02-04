package com.pythian.hive.udf

import java.io.IOException
import java.util.HashMap

import org.apache.hadoop.hive.ql.exec.{Description, UDFArgumentException}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import play.api.libs.json._

@Description(name="json_map", value = "_FUNC_(json) - Returns a map of key-value pairs from a JSON object")
class JsonMapGenericUDF extends GenericUDF {

  var stringInspector: StringObjectInspector = _

  override def initialize(args: Array[ObjectInspector]): ObjectInspector = {
    
    if(!HiveUDFUtils.validateGenericUDFInput(args)) { throw new UDFArgumentException("Usage : json_map(jsonstring) ") }
    
    stringInspector = args(0).asInstanceOf[StringObjectInspector]

    ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
      PrimitiveObjectInspectorFactory.javaStringObjectInspector)
  }

  override def evaluate(args: Array[DeferredObject]): AnyRef = {
    try {
      val jsonString: String = stringInspector.getPrimitiveJavaObject(args(0).get())
      val jValue: JsValue = Json.parse(jsonString)

      unpackJsonObject(jValue)

    } catch {
      case e: IOException => throw new HiveException(e)
      case e: NullPointerException => throw new UDFArgumentException(e)
    }
  }

  override def getDisplayString(args: Array[String]) = s"json_map( $args(0) )"

  def unpackJsonObject(j: JsValue): HashMap[String, String] = j match {
    case JsObject(fields) => processMap(fields.toMap)
    case _ => throw new UDFArgumentException("Usage : json_map(jsonstring) ")
  }

  private def processMap(args: Map[String, JsValue]): HashMap[String, String] = {
    val retMap: HashMap[String, String] = new HashMap[String, String]()
    args foreach {
      case (k, v) => retMap.put(k, HiveUDFUtils.getJsonString(v))
    }
    retMap
  }
}