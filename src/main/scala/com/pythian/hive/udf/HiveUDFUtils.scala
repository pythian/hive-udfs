package com.pythian.hive.udf

import java.util.ArrayList

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import play.api.libs.json._

import scala.collection.JavaConversions._

object HiveUDFUtils {

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
    val json = jsonArray.zipWithIndex.toMap.map{ case (k, v) => Array[Any](v, HiveUDFUtils.getJsonString(k)) }

    new ArrayList[Array[Any]](json)
  }
  
  def validateGenericUDFInput(args: Array[ObjectInspector]): Boolean = {
    
    val h: ObjectInspector = args.toList.headOption.getOrElse( return false )
    val c: Category = h.getCategory()
    val p: PrimitiveCategory = h.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory()
    
    (c,p) match {
      case (Category.PRIMITIVE, PrimitiveCategory.STRING) => true
      case _  => false
    }
  }
}
