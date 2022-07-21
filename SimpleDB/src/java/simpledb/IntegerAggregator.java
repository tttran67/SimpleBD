package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex;//分组字段的序号
    private Type gbFieldType;//分组字段的类型
    private int aggFieldIndex;//聚合字段的序号
    AggHandler aggHandler;//自定义类实现count、sum、max、min、avg
    private Op what;//需要的聚合操作
    private abstract class AggHandler{
        HashMap<Field,Integer> aggResult;
        //用于保存聚合后的结果
        //Filed是用于分组的gbField，gbFieIndex=NO_GROUPING时为null
        //Integer是聚合结果
        abstract void handle(Field gbField, IntField aggField);

        public AggHandler(){
            aggResult = new HashMap<>();
        }
        public HashMap<Field,Integer> getAggResult(){
            return aggResult;
        }
    }

    private class CountHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            if(aggResult.containsKey(gbField)){
                //对相同gbField对应的Integer计数
                //HashMap的put方法覆盖键值对
                //HashMap的get方法获取key对应的value
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            } else {
                aggResult.put(gbField, 1);
            }
        }
    }

    private class SumHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + value);
            } else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class MaxHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, Math.max(aggResult.get(gbField) , value));
            } else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class MinHandler extends AggHandler{

        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, Math.min(aggResult.get(gbField) , value));
            } else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class AvgHandler extends  AggHandler{
        HashMap<Field, Integer> sum;
        HashMap<Field, Integer> count;
        private AvgHandler(){
            sum = new HashMap<>();
            count = new HashMap<>();
        }
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();

            if(sum.containsKey(gbField) && count.containsKey(gbField)){
                sum.put(gbField, sum.get(gbField) + value);
                count.put(gbField, count.get(gbField) + 1);
            } else {
                sum.put(gbField, value);
                count.put(gbField, 1);
            }
            int avg = sum.get(gbField) / count.get(gbField);
            aggResult.put(gbField, avg);
        }
    }
    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        //根据不同的聚合操作Op选择使用aggHandler的不同子类
        gbFieldIndex = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldIndex = afield;
        switch (what) {
            case MIN:
                aggHandler = new MinHandler();
                break;
            case MAX:
                aggHandler = new MaxHandler();
                break;
            case SUM:
                aggHandler = new SumHandler();
                break;
            case COUNT:
                aggHandler = new CountHandler();
                break;
            case AVG:
                aggHandler = new AvgHandler();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator ");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //把1个tuple加入group，需要在遍历时反复调用
        Field gbField;
        IntField aggField = (IntField) tup.getField(aggFieldIndex);
        if(gbFieldIndex == NO_GROUPING){
            gbField = null;
        } else {
            gbField = tup.getField(gbFieldIndex);
        }
        aggHandler.handle(gbField,aggField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */


    public OpIterator iterator() {
        // some code goes here
        //结果集中的tuple一般形式为(groupByValue,aggregateValue)
        HashMap<Field,Integer> result = aggHandler.getAggResult();
        Type[] fieldTypes;
        String[] fieldNames;
        TupleDesc tupleDesc;
        List<Tuple> tuples = new ArrayList<>();
        if(gbFieldIndex == NO_GROUPING){
            //当groupByValue字段的值是NO_GROUPING时，结果的tuple的TupleDesc为(aggregateValue)
            fieldTypes = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{"aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes,fieldNames);
            Tuple tuple = new Tuple(tupleDesc);
            IntField resultField = new IntField(result.get(gbFieldIndex));
            tuple.setField(0,resultField);
            tuples.add(tuple);
        } else {
            //否则TupleDesc为(groupByValue,aggregateValue)
            fieldTypes = new Type[]{gbFieldType,Type.INT_TYPE};
            fieldNames = new String[]{"groupByValue" , "aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes,fieldNames);
            //分组后，要处理group中每个tuple
            for(Field field : result.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbFieldType == Type.INT_TYPE){
                    IntField gbField = (IntField)field;
                    tuple.setField(0,gbField);
                } else {
                    StringField gbField = (StringField) field;
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
