package com.damientseng.sak.hive.ql.udf;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.ISupportStreamingModeForWindowing;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Iterator;


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
        eval.iterate(buffer, new Object[]{new IntWritable(1), new Text("27")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("86")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("24")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(1), new Text("08")});  // 08
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("76")});  // 08

        Object output = eval.terminate(buffer);

        Object[] expected = {null,
                new Text("27"), new Text("27"), new Text("27"),
                new Text("08"), new Text("08")};
        Assert.assertArrayEquals(expected, ((StandardListObjectInspector) loi).getList(output).toArray());

    }

    public void testStreamingRecent() throws HiveException {

        Iterator<Integer> inFlags = Arrays.asList(0, 1, 0, 0, 1, 0).iterator();
        Iterator<String> inSrcs = Arrays.asList("95", "27", "86", "24", "08", "76").iterator();
        Iterator<Text> outVals = Arrays.asList(null,
                new Text("27"), new Text("27"), new Text("27"),
                new Text("08"), new Text("08")).iterator();

        int inSz = 6;

        Object[] in = new Object[2];

        TypeInfo[] inputTypes = {TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo};
        ObjectInspector[] inputOIs = {PrimitiveObjectInspectorFactory.writableIntObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector};

        GenericUDAFRecent fnR = new GenericUDAFRecent();
        GenericUDAFEvaluator fn = fnR.getEvaluator(inputTypes);
        fn.init(GenericUDAFEvaluator.Mode.COMPLETE, inputOIs);
        fn = fn.getWindowingEvaluator(new WindowFrameDef());

        GenericUDAFEvaluator.AggregationBuffer agg = fn.getNewAggregationBuffer();

        ISupportStreamingModeForWindowing oS = (ISupportStreamingModeForWindowing) fn;

        int outSz = 0;
        while (inFlags.hasNext()) {
            in[0] = new IntWritable(inFlags.next());
            in[1] = new Text(inSrcs.next());

            fn.aggregate(agg, in);
            Object out = oS.getNextResult(agg);
            assertEquals(out, outVals.next());
            outSz++;
        }

        fn.terminate(agg);
        assertEquals(outSz, inSz);
    }

}