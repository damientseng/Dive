package com.damientseng.dive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.Serializable;
import java.util.ArrayList;


public class GenericUDAFCompareWhenEvaluator extends GenericUDAFEvaluator
        implements Serializable {

    private static final long serialVersionUID = 1L;

    enum Order {MAX, MIN}

    private int sign;

    public GenericUDAFCompareWhenEvaluator(Order order) {
        this.sign = (order.equals(Order.MAX) ? 1 : -1);
    }

    public static void paramCheck(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly two argument is expected.");
        }
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(0,
                    "Cannot support comparison of map<> type or complex type containing map<>.");
        }
    }

    private transient ObjectInspector cmpInputOI;
    private transient ObjectInspector resInputOI;
    private transient ObjectInspector cmpOutputOI;
    private transient ObjectInspector resOutputOI;
    private transient StructField cmpField;
    private transient StructField resField;
    private transient StructObjectInspector soi;
    // For PARTIAL1 and PARTIAL2
    protected transient Object[] partialResult;

    @Override
    public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
            throws HiveException {
        super.init(mode, parameters);

        if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
            assert (parameters.length == 2);
            cmpInputOI = parameters[0];
            resInputOI = parameters[1];
            cmpOutputOI = ObjectInspectorUtils.getStandardObjectInspector(cmpInputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            resOutputOI = ObjectInspectorUtils.getStandardObjectInspector(resInputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        } else {
            assert (parameters.length == 1);
            soi = (StructObjectInspector) parameters[0];
            cmpField = soi.getStructFieldRef("cmp");
            resField = soi.getStructFieldRef("res");
            cmpOutputOI = cmpField.getFieldObjectInspector();
            resOutputOI = resField.getFieldObjectInspector();
        }

        if (mode == Mode.PARTIAL1) {
            ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
            foi.add(cmpOutputOI);
            foi.add(resOutputOI);
            ArrayList<String> fname = new ArrayList<String>();
            fname.add("cmp");
            fname.add("res");
            soi = ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
        }

        if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
            if (partialResult == null) {
                partialResult = new Object[2];
            }
            return soi;
        } else {
            return resOutputOI;
        }
    }

    static class WhenAgg extends AbstractAggregationBuffer {
        Object cmp;
        Object res;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new WhenAgg();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        WhenAgg myagg = (WhenAgg) agg;
        myagg.cmp = null;
        myagg.res = null;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
            throws HiveException {
        assert (parameters.length == 2);
        if (parameters[0] == null || parameters[1] == null) {
            return;
        }

        WhenAgg myagg = (WhenAgg) agg;
        int r = this.sign * ObjectInspectorUtils.compare(myagg.cmp, cmpOutputOI, parameters[0], cmpInputOI);
        if (myagg.cmp == null || r < 0) {
            myagg.cmp = ObjectInspectorUtils.copyToStandardObject(parameters[0], cmpInputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            myagg.res = ObjectInspectorUtils.copyToStandardObject(parameters[1], resInputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        WhenAgg myagg = (WhenAgg) agg;
        partialResult[0] = myagg.cmp;
        partialResult[1] = myagg.res;
        return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
            throws HiveException {
        if (partial != null) {
            WhenAgg myagg = (WhenAgg) agg;
            Object partCmp = soi.getStructFieldData(partial, cmpField);
            int r = this.sign * ObjectInspectorUtils.compare(myagg.cmp, cmpOutputOI, partCmp, cmpOutputOI);
            if (myagg.cmp == null || r < 0) {
                myagg.cmp = partCmp;
                myagg.res = soi.getStructFieldData(partial, resField);
            }
        }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
        WhenAgg myagg = (WhenAgg) agg;
        return myagg.res;
    }
}
