package com.damientseng.dive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.UDTFCollector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class GenericUDTFDuplicateTest {

  @Test
  public void testGenericUDTFDuplicateType() {
    @SuppressWarnings("resource")
    GenericUDTFDuplicate tf = new GenericUDTFDuplicate();
    ObjectInspector[] arguments = {
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector};

    try {
      tf.initialize(arguments);
      fail("GenericUDTFDuplicateTest. UDFArgumentException is expected");
    } catch (UDFArgumentException e) {
      assertEquals("GenericUDTFDuplicateTest first arg",
          "The first argument to DUP() must be a constant integer (got string instead).", e.getMessage());
    }

    PrimitiveTypeInfo pt = new PrimitiveTypeInfo();
    pt.setTypeName("int");
    arguments[0] = getPrimitiveWritableConstantObjectInspector(pt, new IntWritable(0));
    try {
      tf.initialize(arguments);
      fail("GenericUDTFDuplicateTest. UDFArgumentException is expected");
    } catch (UDFArgumentException e) {
      assertEquals("GenericUDTFDuplicateTest first arg",
          "DUP() expects its first argument to be >= 1.", e.getMessage());
    }

  }

  @Test
  public void testProcess() throws HiveException {
    //  tf.setCollector(new MyUDTFCollector(new UDTFOperator(new CompilationOpContext())));
    GenericUDTFDuplicate tf = new GenericUDTFDuplicate();
    UDTFCollector collector = Mockito.mock(UDTFCollector.class);
    tf.setCollector(collector);
    PrimitiveTypeInfo pt = new PrimitiveTypeInfo();
    pt.setTypeName("int");
    int times = 3;
    ObjectInspector[] args = {
        getPrimitiveWritableConstantObjectInspector(pt, new IntWritable(times)),
        writableStringObjectInspector,
        writableIntObjectInspector,
        writableDoubleObjectInspector};
    tf.initialize(args);
    tf.process(new Object[]{
        new IntWritable(times),
        new Text("ace"),
        new IntWritable(4),
        new DoubleWritable(9.5d)});

    Mockito.verify(collector, Mockito.times(times))
        .collect(MockitoHamcrest
            .argThat(equalTo(new Object[]{
                new Text("ace"),
                new IntWritable(4),
                new DoubleWritable(9.5d)})));

  }
}