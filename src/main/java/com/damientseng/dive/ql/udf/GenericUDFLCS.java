package com.damientseng.dive.ql.udf;

import org.apache.commons.text.similarity.LongestCommonSubsequence;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;


/**
 * GenericUDFLCS
 */
@Description(name = "lcs", value = "_FUNC_(str1, str2) - "
    + "Calculate the size of the longest common sub-sequence of str1 and str2"
    + "Example:\n "
    + " > SELECT _FUNC_('abcde', 'aced');\n 3")
public class GenericUDFLCS extends GenericUDF {
  private final IntWritable output = new IntWritable();
  private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
  private transient LongestCommonSubsequence lcsUtil;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP, VOID_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);
    obtainStringConverter(arguments, 1, inputTypes, converters);

    lcsUtil = new LongestCommonSubsequence();

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String str0 = getStringValue(arguments, 0, converters);
    String str1 = getStringValue(arguments, 1, converters);

    if (str0 == null || str1 == null) {
      return null;
    }

    int length = lcsUtil.apply(str0, str1);
    output.set(length);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "lcs";
  }

}

