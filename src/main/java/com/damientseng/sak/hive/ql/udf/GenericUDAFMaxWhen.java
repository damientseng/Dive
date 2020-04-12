package com.damientseng.sak.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


@Description(name = "maxwhen", value = "_FUNC_(cmp, res) - Returns res from the row with the maximum value of cmp")
public class GenericUDAFMaxWhen extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFMaxWhen.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        GenericUDAFCompareWhenEvaluator.paramCheck(parameters);
        return new GenericUDAFCompareWhenEvaluator(GenericUDAFCompareWhenEvaluator.Order.MAX);
    }

}
