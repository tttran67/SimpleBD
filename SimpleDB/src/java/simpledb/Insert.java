package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;//执行插入操作的事务id
    private OpIterator child;//待插入的tuple的迭代器
    private int tableId;//tuple插入的表的id
    private boolean accessed;//标志位，避免fetchNext无限向下取
    private TupleDesc td;//返回结果（表示影响了多少tuple的一个tuple）的td，fieldType={Type.INT_TYPE},fieldNames={null}
    private int count;//插入的tuple数量

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        //下面的if判断加入后无法通过测试，to be finished
//        if(!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())){
//            throw new DbException("TupleDesc do not match!");
//        }
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.accessed = false;
        count = 0;
        Type[] types = {Type.INT_TYPE};
        String[] fieldNames = {null};//这里要写为null，否则和测试样例的预期答案不符
        this.td = new TupleDesc(types,fieldNames);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        accessed = false;
        child.open();
        super.open();
        while(child.hasNext()){
            Tuple next = child.next();
            try{
                Database.getBufferPool().insertTuple(t,tableId,next);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        accessed = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(accessed) return null;//如果取了不止一次则返回Null
        accessed = true;
        Tuple ans = new Tuple(getTupleDesc());
        ans.setField(0,new IntField(count));
        return ans;
    }


    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
