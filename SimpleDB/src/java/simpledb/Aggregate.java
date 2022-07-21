package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;//待聚合的tuple
    private int aggFieldIndex;//待聚合字段的序号
    private int gbFieldIndex;//分组字段的序号
    private Aggregator.Op aop;//运算符

    private Aggregator aggregator;//进行聚合操作的类
    private OpIterator aggIterator;//聚合结果的迭代器
    private TupleDesc aggTupleDesc;//聚合结果的属性行

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        aggFieldIndex = afield;
        gbFieldIndex = gfield;
        this.aop = aop;
        Type gbFieldType = gbFieldIndex == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gbFieldIndex);
        Type aggFieldType = child.getTupleDesc().getFieldType(aggFieldIndex);
        Type[] fieldTypes ;
        String[] fieldNames;
        String aggFieldName = child.getTupleDesc().getFieldName(aggFieldIndex);
        if(gbFieldType == null){
            //no_grouping只需初始化aggfieldtype、aggfieldname
            fieldTypes = new Type[]{aggFieldType};
            fieldNames = new String[]{aggFieldName};
        } else {
            //grouping还需初始化gbfiledtyoe、gbfieldname
            fieldTypes = new Type[]{gbFieldType,aggFieldType};
            String gbFieldName = child.getTupleDesc().getFieldName(gbFieldIndex);
            fieldNames = new String[]{gbFieldName,aggFieldName};
        }
        //得到聚合后的tupledesc方便后续使用
        aggTupleDesc = new TupleDesc(fieldTypes,fieldNames);
    }


    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        return this.gbFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        return this.aggTupleDesc.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return this.aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        //tuple没有Groupby时取第一个field，否则取第二个
        if(this.gbFieldIndex == -1)
            return this.aggTupleDesc.getFieldName(0);
        else
            return this.aggTupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();

        Type gbFieldType = gbFieldIndex == -1 ? null : child.getTupleDesc().getFieldType(gbFieldIndex);
        Type aggFieldType = child.getTupleDesc().getFieldType(aggFieldIndex);
        if(aggFieldType == Type.INT_TYPE){
            //整型field的聚合
            aggregator = new IntegerAggregator(gbFieldIndex,gbFieldType,aggFieldIndex,aop);
        } else {
            //string类型field的聚合
            aggregator = new StringAggregator(gbFieldIndex,gbFieldType,aggFieldIndex,aop);
        }
        while (child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }

        aggIterator = aggregator.iterator();
        aggIterator.open();
    }


    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.aggIterator.hasNext())
            return this.aggIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.aggTupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.aggIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}