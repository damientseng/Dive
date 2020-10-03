package com.damientseng.dive.ql.udf;

import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

/**
 * GenericUDFLCS
 */
@Description(name = "jaccard", value = "_FUNC_(str1, str2) - "
    + "Calculate the Jaccard Similarity of str1 and str2"
    + "Example:\n "
    + " > SELECT _FUNC_('abcde', 'aced');\n 0.8")
public class GenericUDFJaccard extends GenericUDF {
  private final DoubleWritable output = new DoubleWritable();
  private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
  private transient JaccardSimilarity lcsUtil;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP, VOID_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);
    obtainStringConverter(arguments, 1, inputTypes, converters);

    lcsUtil = new JaccardSimilarity();

    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String str0 = getStringValue(arguments, 0, converters);
    String str1 = getStringValue(arguments, 1, converters);

    if (str0 == null || str1 == null) {
      return null;
    }

    double jsim = lcsUtil.apply(str0, str1);
    output.set(jsim);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "jaccard";
  }

}

