package com.damientseng.dive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenericUDTFDuplicate extends GenericUDTF {

  IntWritable numRows = null;
  Integer numCols = null;
  private transient List<ObjectInspector> argOIs = new ArrayList<ObjectInspector>();
  private transient Object[] forwardObj = null;
  private transient ArrayList<GenericUDFUtils.ReturnObjectInspectorResolver> returnOIResolvers =
      new ArrayList<GenericUDFUtils.ReturnObjectInspectorResolver>();

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {
    if (args.length < 2) {
      throw new UDFArgumentException("DUP() expects at least two arguments.");
    }
    if (!(args[0] instanceof ConstantObjectInspector)) {
      throw new UDFArgumentException(
          "The first argument to DUP() must be a constant integer (got " +
              args[0].getTypeName() + " instead).");
    }
    numRows = (IntWritable) ((ConstantObjectInspector) args[0]).getWritableConstantValue();
    numCols = args.length - 1;

    if (numRows == null || numRows.get() < 1) {
      throw new UDFArgumentException(
          "DUP() expects its first argument to be >= 1.");
    }

    argOIs.addAll(Arrays.asList(args));

    for (int i = 0; i < numCols; ++i) {
      returnOIResolvers.add(new GenericUDFUtils.ReturnObjectInspectorResolver());
      returnOIResolvers.get(i).update(args[i + 1]);
    }

    forwardObj = new Object[numCols];

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    for (int i = 0; i < numCols; ++i) {
      fieldNames.add("col" + i);
      fieldOIs.add(returnOIResolvers.get(i).get());
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
        fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    for (int j = 1; j < args.length; ++j) {
      forwardObj[j - 1] =
          returnOIResolvers.get(j - 1).convertIfNecessary(args[j], argOIs.get(j));
    }
    for (int i = 0; i < numRows.get(); ++i)
      forward(forwardObj);
  }

  @Override
  public String toString() {
    return "dup";
  }

}
