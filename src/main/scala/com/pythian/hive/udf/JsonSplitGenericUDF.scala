package com.pythian.hive.udf

import java.io.IOException
import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.{Description, UDFArgumentException}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}

@Description(name = "json_split", value = "_FUNC_(json) - Returns a array of JSON strings from a JSON Array")
class JsonSplitGenericUDF extends GenericUDF {

  var stringInspector: StringObjectInspector = _

  override def initialize(args: Array[ObjectInspector]): ObjectInspector = {

    if(!HiveUDFUtils.validateGenericUDFInput(args)) { throw new UDFArgumentException("Usage : json_split(jsonstring) ") }

    val RowId = "row_id"
    val JsonString = "json_string"

    stringInspector = args(0).asInstanceOf[StringObjectInspector]

    val outputColumns: ArrayList[String] = new ArrayList[String]()
    outputColumns.add(RowId)
    outputColumns.add(JsonString)

    val outputTypes = new ArrayList[ObjectInspector]()
    outputTypes.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector)
    outputTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardListObjectInspector(
      ObjectInspectorFactory.getStandardStructObjectInspector(outputColumns, outputTypes))
  }

  override def evaluate(args: Array[DeferredObject]): AnyRef = {

    try {
      val jsonString: String = stringInspector.getPrimitiveJavaObject(args(0).get())
      splitJsonString(jsonString)

    } catch {
      case e: IOException => throw new HiveException(e)
      case e: NullPointerException => throw new UDFArgumentException(e)
    }
  }

  override def getDisplayString(args: Array[String]) = "json_split(" + args(0) + ")"

  def splitJsonString(jsonString: String): ArrayList[Array[Any]] = HiveUDFUtils.processJsonString(jsonString)

}
