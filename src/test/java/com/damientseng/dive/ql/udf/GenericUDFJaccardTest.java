package com.damientseng.dive.ql.udf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class GenericUDFJaccardTest extends TestCase {

  public void testJaccard() throws HiveException {
    GenericUDFJaccard udf = new GenericUDFJaccard();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI0, valueOI1};

    udf.initialize(arguments);
    apply("abcde", "aced", 0.8d, udf);
    apply("abc", "abc", 1d, udf);
    apply("abc", "def", 0d, udf);

    apply("abc", "", 0d, udf);
    apply("", "abc", 0d, udf);
  }

  private void apply(String str0, String str1, Double expect, GenericUDF udf)
      throws HiveException {
    GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(new Text(str0));
    GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(new Text(str1));
    GenericUDF.DeferredObject[] args = {valueObj0, valueObj1};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertNotNull("jaccard test", output);
    System.out.println(output.get());
    assertEquals("jaccard test", expect, output.get());
  }
}
