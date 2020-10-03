package com.damientseng.dive.ql.udf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class GenericUDFLCSTest extends TestCase {

  public void testLCS() throws HiveException {
    GenericUDFLCS udf = new GenericUDFLCS();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI0, valueOI1};

    udf.initialize(arguments);
    apply("abcde", "aced", 3, udf);
    apply("abc", "abc", 3, udf);
    apply("abc", "def", 0, udf);

    apply("abc", "", 0, udf);
    apply("", "abc", 0, udf);
  }

  private void apply(String str0, String str1, Integer expect, GenericUDF udf)
      throws HiveException {
    GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(new Text(str0));
    GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(new Text(str1));
    GenericUDF.DeferredObject[] args = {valueObj0, valueObj1};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertNotNull("lcs test", output);
    assertEquals("lcs test", expect.intValue(), output.get());
  }
}
