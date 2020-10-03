package com.damientseng.dive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFStack;
import org.apache.hadoop.hive.ql.udf.generic.UDTFCollector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GenericUDTFStackTest {

  @Test
  public void testStackNumOfArgs() {
    GenericUDTFStack tf = new GenericUDTFStack();
    UDTFCollector collector = Mockito.mock(UDTFCollector.class);
    tf.setCollector(collector);
    PrimitiveTypeInfo pt = new PrimitiveTypeInfo();
    pt.setTypeName("int");
    int times = 4;
    ObjectInspector[] args = {
        getPrimitiveWritableConstantObjectInspector(pt, new IntWritable(times)),
        writableIntObjectInspector,
        writableDoubleObjectInspector};

    try {
      tf.initialize(args);
      fail("GenericUDTFStack num of args mismatch");
    } catch (UDFArgumentException e) {
      assertEquals("GenericUDTFStack num of args",
          "Argument 1's type (int) should be equal to argument 2's type (double)", e.getMessage());
    }

  }

}
