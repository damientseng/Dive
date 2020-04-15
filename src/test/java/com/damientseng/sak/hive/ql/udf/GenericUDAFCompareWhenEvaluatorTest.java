package com.damientseng.sak.hive.ql.udf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class GenericUDAFCompareWhenEvaluatorTest extends TestCase {

    public void testMaxWhen() throws HiveException {
        GenericUDAFMaxWhen maxWhen = new GenericUDAFMaxWhen();

        GenericUDAFEvaluator eval1 = maxWhen.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo});
        GenericUDAFEvaluator eval2 = maxWhen.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.stringTypeInfo, TypeInfoFactory.intTypeInfo});

        ObjectInspector poi1 = eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector});

        ObjectInspector poi2 = eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector});

        GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
        eval1.iterate(buffer1, new Object[]{new Text("12"), new IntWritable(2)});
        eval1.iterate(buffer1, new Object[]{null, new IntWritable(9)});
        eval1.iterate(buffer1, new Object[]{new Text("20"), null});
        eval1.iterate(buffer1, new Object[]{new Text("18"), new IntWritable(8)});
        Object object1 = eval1.terminatePartial(buffer1);

        GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
        eval2.iterate(buffer2, new Object[]{new Text("16"), new IntWritable(6)});
        eval2.iterate(buffer2, new Object[]{new Text("17"), new IntWritable(7)});
        eval2.iterate(buffer2, new Object[]{new Text("15"), new IntWritable(5)});
        Object object2 = eval2.terminatePartial(buffer2);

        ObjectInspector coi = eval2.init(GenericUDAFEvaluator.Mode.FINAL,
                new ObjectInspector[]{poi1});

        GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
        eval2.merge(buffer3, object2);
        eval2.merge(buffer3, object1);

        Object result = eval2.terminate(buffer3);
        assertEquals(8, ((JavaIntObjectInspector) coi).get(result));

    }

}