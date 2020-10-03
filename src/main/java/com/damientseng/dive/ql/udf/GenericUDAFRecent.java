package com.damientseng.dive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.ISupportStreamingModeForWindowing;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;


@WindowFunctionDescription(
    description = @Description(
        name = "recent",
        value = "_FUNC_(flg, src) - "
    ),
    supportsWindow = false,
    pivotResult = true
)
public class GenericUDAFRecent extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFRecent.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {

    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly two argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }

    return new GenericUDAFRecentEvaluator();
  }

  static class RctAgg implements AggregationBuffer {
    ArrayList<Object> srcs;
    Object currSrc;
    ObjectInspector srcOI;
    boolean supportsStreaming;

    RctAgg(ObjectInspector srcOI, boolean supportsStreaming) {
      this.srcOI = srcOI;
      this.supportsStreaming = supportsStreaming;
      init();
    }

    void init() {
      srcs = new ArrayList<Object>();
      currSrc = null;
      if (supportsStreaming) {
        srcs.add(null);
      }
    }

    void add() {
      if (supportsStreaming) {
        srcs.set(0, ObjectInspectorUtils.copyToStandardObject(currSrc, srcOI,
            ObjectInspectorUtils.ObjectInspectorCopyOption.DEFAULT));

      } else {
        srcs.add(ObjectInspectorUtils.copyToStandardObject(currSrc, srcOI,
            ObjectInspectorUtils.ObjectInspectorCopyOption.DEFAULT));
      }

    }

    void update(Object src) {
      this.currSrc = src;
    }
  }

  public static class GenericUDAFAbstractRecentEvaluator extends GenericUDAFEvaluator {

    PrimitiveObjectInspector flgInputOI;
    ObjectInspector srcInputOI;

    boolean isStreamingMode = false;

    protected boolean isStreaming() {
      return isStreamingMode;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m != Mode.COMPLETE) {
        throw new HiveException("Only COMPLETE mode supported");
      }
      flgInputOI = (PrimitiveObjectInspector) parameters[0];
      srcInputOI = parameters[1];
      return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils
          .getStandardObjectInspector(srcInputOI,
              ObjectInspectorUtils.ObjectInspectorCopyOption.DEFAULT));
    }

    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new RctAgg(srcInputOI, isStreamingMode);
    }

    public void reset(AggregationBuffer agg) throws HiveException {
      ((RctAgg) agg).init();
    }

    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 2);
      RctAgg myagg = (RctAgg) agg;
      int flg = PrimitiveObjectInspectorUtils.getInt(parameters[0], flgInputOI);

      if (flg == 0) {
        // if record is from anchor table
        myagg.add();
      } else if (flg == 1) {
        // if from look table, update current src and then add
        myagg.update(parameters[1]);
        myagg.add();
      } else {
        throw new HiveException("unknown flag: " + flg + " must be 0 or 1");
      }

    }

    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      throw new HiveException("terminatePartial not supported");
    }

    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      throw new HiveException("merge not supported");
    }

    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((RctAgg) agg).srcs;
    }

  }

  public static class GenericUDAFRecentEvaluator extends GenericUDAFAbstractRecentEvaluator
      implements ISupportStreamingModeForWindowing {

    public Object getNextResult(AggregationBuffer agg) throws HiveException {
      return ((RctAgg) agg).srcs.get(0);
    }

    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      isStreamingMode = true;
      return this;
    }

    public int getRowsRemainingAfterTerminate() throws HiveException {
      return 0;
    }
  }

}
