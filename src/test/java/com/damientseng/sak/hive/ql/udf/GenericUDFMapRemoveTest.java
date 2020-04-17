package com.damientseng.sak.hive.ql.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class GenericUDFMapRemoveTest {

    @Test
    public void testMapRemove() throws HiveException, IOException {
        ObjectInspector[] inputOIs = {
                ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector),

                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };

        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 3);


        GenericUDF.DeferredObject[] args = {
                new GenericUDF.DeferredJavaObject(map),
                new GenericUDF.DeferredJavaObject("a")};

        GenericUDFMapRemove udf = new GenericUDFMapRemove();
        StandardMapObjectInspector oi = (StandardMapObjectInspector) udf.initialize(inputOIs);
        Map res = oi.getMap(udf.evaluate(args));
        assertFalse(res.containsKey("a"));
        assertTrue(res.containsKey("b") && (Integer) res.get("b") == 2);
        assertTrue(res.containsKey("c") && (Integer) res.get("c") == 3);

        args = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(map),
                new GenericUDF.DeferredJavaObject("b")};
        res = oi.getMap(udf.evaluate(args));
        assertTrue(res.containsKey("a") && (Integer) res.get("a") == 1);
        assertFalse(res.containsKey("b"));
        assertTrue(res.containsKey("c") && (Integer) res.get("c") == 3);

        udf.close();
    }

}