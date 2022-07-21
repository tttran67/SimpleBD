package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;
/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc td;
    private RecordId recordId;
    private List<Field> fields;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {//构造函数
        // some code goes here
        this.td = td;
        this.fields = new ArrayList<>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {//获得Tuple对应的TupleDesc
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {//获得元组id
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {//设置元组id
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {//为第i个字段赋值
        // some code goes here
        if(i == fields.size()){
            fields.add(f);
            return;
        }
        if(i < 0 || i > fields.size())
            throw new NoSuchElementException("Field索引值不合法");
        fields.set(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {//获得第i个字段
        // some code goes here
        if(i < 0 || i >= fields.size())
            throw new NoSuchElementException("Field索引值不合法");
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {//toString()方法返回所有字段
        // some code goes here
        StringBuilder res = new StringBuilder();
        for(Field field : this.fields){
            res.append(field.toString());
            res.append(" ");
        }
        String ret = res.toString();
        if(fields.size() != 0)
            ret = ret.substring(0,res.length() - 1);
        return ret;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()//返回字段的迭代器
    {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)//重置TupleDesc
    {
        // some code goes here
        this.td = td;
    }
}
