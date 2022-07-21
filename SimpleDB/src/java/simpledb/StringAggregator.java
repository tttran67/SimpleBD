package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex;
    private Type gbFieldType;
    private int aggFieldIndex;
    StringAggregator.AggHandler aggHandler;
    private Op what;

    private abstract class AggHandler{
        HashMap<Field,Integer> aggResult;
        //用于保存聚合后的结果
        //Filed是用于分组的gbField，gbFieIndex=NO_GROUPING时为null
        //Integer是聚合结果
        abstract void handle(Field gbField, StringField aggField);

        public AggHandler(){
            aggResult = new HashMap<>();
        }
        public HashMap<Field,Integer> getAggResult(){
            return aggResult;
        }
    }

    private class CountHandler extends StringAggregator.AggHandler {

        @Override
        void handle(Field gbField, StringField aggField) {
            if(aggResult.containsKey(gbField)){
                //对相同gbField对应的String计数
                //HashMap的put方法覆盖键值对
                //HashMap的get方法获取key对应的value
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            } else {
                aggResult.put(gbField, 1);
            }
        }
    }
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbFieldIndex = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldIndex = afield;
        switch (what) {
            case COUNT:
                aggHandler = new StringAggregator.CountHandler();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator ");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field gbField;
        StringField aggField = (StringField) tup.getField(aggFieldIndex);
        if(gbFieldIndex == NO_GROUPING){
            gbField = null;
        } else {
            gbField = tup.getField(gbFieldIndex);
        }
        String newValue = aggField.getValue();
        aggHandler.handle(gbField,aggField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //这里的type注意不要混淆，聚合后的结果是数字INT_TYPE，原tuple的field是STRING_TYPE
        HashMap<Field,Integer> result = aggHandler.getAggResult();
        Type[] fieldTypes;
        String[] fieldNames;
        TupleDesc tupleDesc;
        List<Tuple> tuples = new ArrayList<>();
        if(gbFieldIndex == NO_GROUPING){
            //当groupByValue字段的值是NO_GROUPING时，结果的tuple只有(aggregateValue)一个字段
            fieldTypes = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{"aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes,fieldNames);
            Tuple tuple = new Tuple(tupleDesc);
            IntField resultField = new IntField(result.get(gbFieldIndex));
            tuple.setField(0,resultField);
            tuples.add(tuple);
        } else {
            fieldTypes = new Type[]{gbFieldType,Type.INT_TYPE};
            fieldNames = new String[]{"groupByValue" , "aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes,fieldNames);
            for(Field field : result.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbFieldType == Type.STRING_TYPE){
                    StringField gbField = (StringField)field;
                    tuple.setField(0,gbField);
                } else {
                    IntField gbField = (IntField) field;
                    tuple.setField(0,gbField);
                }

                IntField resultField = new IntField(result.get(field));
                tuple.setField(1,resultField);
                tuples.add(tuple);
            }
        }
        //由于需要的返回类型是OpIterator，因此使用TupleIterator进行封装
        return new TupleIterator(tupleDesc,tuples);
    }

}
