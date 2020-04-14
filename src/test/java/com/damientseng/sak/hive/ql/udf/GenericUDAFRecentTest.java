package com.damientseng.sak.hive.ql.udf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;


public class GenericUDAFRecentTest extends TestCase {

    public void testRecent() throws HiveException {
        GenericUDAFRecent recent = new GenericUDAFRecent();

        GenericUDAFEvaluator eval = recent.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo});

        ObjectInspector loi = eval.init(GenericUDAFEvaluator.Mode.COMPLETE,
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableIntObjectInspector,
                        PrimitiveObjectInspectorFactory.writableStringObjectInspector});

        GenericUDAFEvaluator.AggregationBuffer buffer = eval.getNewAggregationBuffer();

        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("95")});  // null
        eval.iterate(buffer, new Object[]{new IntWritable(1), new Text("27")});  // null
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("86")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("24")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(1), new Text("08")});  // null
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("76")});  // 08

        Object output = eval.terminate(buffer);

        Object[] expected = {null, null, new Text("27"), new Text("27"), null, new Text("08")};
        Assert.assertArrayEquals(expected, ((StandardListObjectInspector) loi).getList(output).toArray());

    }

}