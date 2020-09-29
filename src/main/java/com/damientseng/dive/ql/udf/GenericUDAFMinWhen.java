package com.damientseng.dive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


@Description(name = "minwhen", value = "_FUNC_(cmp, res) - Returns res from the row with the minimum value of cmp")
public class GenericUDAFMinWhen extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFMinWhen.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        GenericUDAFCompareWhenEvaluator.paramCheck(parameters);
        return new GenericUDAFCompareWhenEvaluator(GenericUDAFCompareWhenEvaluator.Order.MIN);
    }

}
