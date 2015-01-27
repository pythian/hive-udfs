package com.pythian.hive.udf

import java.io.IOException
import java.util.HashMap

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector}
import play.api.libs.json._

class JsonMapGenericUDF extends GenericUDF {

  var stringInspector: StringObjectInspector = _

  override def initialize(args: Array[ObjectInspector]): ObjectInspector = {
    if(args.length != 1
      || ! args(0).getCategory().equals(Category.PRIMITIVE)
      || args(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory() != PrimitiveCategory.STRING) {
      throw new UDFArgumentException("Usage : json_map(jsonstring) ")
    }
    stringInspector = args(0).asInstanceOf[StringObjectInspector]

    return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                                                PrimitiveObjectInspectorFactory.javaStringObjectInspector)
  }

  override def evaluate(args: Array[DeferredObject]): AnyRef = {
    try {
      val jsonString: String = stringInspector.getPrimitiveJavaObject(args(0).get())
      val jValue: JsValue = Json.parse(jsonString)

      return unpackJsonObject(jValue)

    } catch {
      case e: IOException => throw new HiveException(e)
      case e: NullPointerException => return null
    }
  }

  override def getDisplayString(args: Array[String]): String = {
    return "json_map(" + args(0) + ")"
  }

  def unpackJsonObject(j: JsValue): HashMap[String, String] = j match {
      case JsObject(fields) => processMap(fields.toMap)
      case _ => throw new UDFArgumentException("Usage : json_map(jsonstring) ")
  }

  private def processMap(args: Map[String, JsValue]): HashMap[String, String] = {

    val retMap: HashMap[String, String] = new HashMap[String, String]()

    args foreach {
      case (k, v) => retMap.put(k, getJsonString(v))
    }
    return retMap
  }

  // JsString types does not need explicit conversion
  private def getJsonString(j: JsValue): String = j match {
    case JsString(_) => j.asOpt[String].get
    case _ => j.toString()
  }
}

