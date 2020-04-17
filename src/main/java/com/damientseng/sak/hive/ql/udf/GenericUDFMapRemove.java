package com.damientseng.sak.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.Map;


@Description(name = "map_remove", value = "_FUNC_(map, key0, key1...) - "
        + "Remove the given keys from map")
public class GenericUDFMapRemove extends GenericUDF {

    private transient MapObjectInspector mapOI;
    private transient PrimitiveObjectInspector keyOI;
    private transient ObjectInspectorConverters.Converter converter;

    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException(
                    "The function requires at least two arguments, got "
                            + arguments.length);
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentTypeException(0,
                    "Only map type arguments are accepted but "
                            + arguments[0].getTypeName() + " was passed as parameter 1");
        }
        for (int i = 1; i < arguments.length; i++) {
            if (arguments[i].getCategory() !=
                    ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only primitive type arguments are accepted but "
                                + arguments[i].getTypeName() + " was passed as parameter " + (i + 1));
            }
        }

        mapOI = (MapObjectInspector) arguments[0];
        keyOI = (PrimitiveObjectInspector) mapOI.getMapKeyObjectInspector();
        converter = ObjectInspectorConverters.getConverter(arguments[1], keyOI);
        return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI,
                mapOI.getMapValueObjectInspector());
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object mapObj = arguments[0].get();
        Map<?, ?> mapVal = (Map<?, ?>) ObjectInspectorUtils.copyToStandardObject(mapObj, mapOI);

        for (int i = 1; i < arguments.length; i++) {
            mapVal.remove(converter.convert(arguments[i].get()));
        }
        return mapVal;
    }

    public String getDisplayString(String[] children) {
        assert children.length >= 2;
        return getStandardDisplayString("map_remove", children);
    }
}
