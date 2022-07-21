package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    private final TransactionId tid;//事务id
    private int tableId;//欲扫描的表的id
    private String tableAlias;//表的别名
    private DbFileIterator iterator;//用于遍历表中所有tuple
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        iterator = table.iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //以alias.fieldName形式返回，用于连接包含同名字段的table

        //获取原来的tupleDesc和field个数
        TupleDesc tupleDesc=Database.getCatalog().getTupleDesc(tableId);
        int fieldNum=tupleDesc.numFields();

        //为新的TupleDesc新建Type[]（fieldType）和String[]（fieldName）
        Type[] types=new Type[fieldNum];
        String[] newNames=new String[fieldNum];

        for (int i=0;i<fieldNum;i++){
            types[i]=tupleDesc.getFieldType(i);//type不变
            String ordName=tupleDesc.getFieldName(i);//把原来的fieldname提取出来
            //设置前缀pre
            String pre="null.";
            if(tableAlias!=null){
                pre=tableAlias+".";
            }
            newNames[i]=pre+ordName;//新fieldName
        }
        //用新的fieldType和fieldName建立新的TupleDesc
        return new TupleDesc(types,newNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(iterator != null) return iterator.hasNext();
        throw new TransactionAbortedException();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple = iterator.next();
        if(tuple != null) return tuple;
        else throw new NoSuchElementException("This is the last element");
    }

    public void close() {
        // some code goes here
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator.rewind();
    }
}
