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

      //START, can be broken off here and added to JsonUtil object

      var map: HashMap[String, String] = new HashMap[String, String]()

      //END

      return "call to JsonMapGenericUDFSpec.evaluate(jsonString)"

    } catch {
      case e: IOException => throw new HiveException(e)
      case e: NullPointerException => return null
    }
  }

  override def getDisplayString(args: Array[String]): String = {
    return "json_map(" + args(0) + ")"
  }
}