package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.Arrays;
/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    private List<TDItem> tdItemList = new ArrayList<>();
    private HashMap<String, Integer> name2IndexMap = new HashMap<>();
    public Iterator<TDItem> iterator() {//返回所有属性的迭代器
        // some code goes here

        return this.tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {//初始化方法
        // some code goes here
        for(int i = 0;i < typeAr.length; i++){
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            this.tdItemList.add(tdItem);
            name2IndexMap.put(fieldAr[i], i);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {//无fieldAr的初始化方法
        // some code goes here
        for(Type type : typeAr){
            TDItem tdItem = new TDItem(type, null);
            this.tdItemList.add(tdItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {//返回TupleDesc中属性的数量
        // some code goes here
        return tdItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {//返回第i个属性的属性名
        // some code goes here
        if(i < 0 || i >= numFields()){
            throw new NoSuchElementException(String.format("第%d个文件不存在",i));
        }
        return tdItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {//返回第i个属性的属性类型
        // some code goes here
        if(i < 0 || i >= numFields()){
            throw new NoSuchElementException(String.format("第%d个文件不存在",i));
        }
        return tdItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {//根据属性名返回它在tdItem中的序号
        // some code goes here
        if(this.name2IndexMap.get(name) == null)
            throw new NoSuchElementException(String.format("名为%s的文件不存在",name));
        return this.name2IndexMap.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {//返回元组所占的字节大小
        // some code goes here
        int size = 0;
        for(TDItem tdItem : tdItemList){
            size += tdItem.fieldType.getLen();
            //使用getLen()获得字节数
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {//合并两个tdItemlist
        // some code goes here
        int numFields = td1.numFields() + td2.numFields();
        Type[ ] typeAr = new Type[numFields];
        String[ ] fieldAr = new String[numFields];

        int index = 0;
        for(TDItem tdItem : td1.tdItemList){
            typeAr[index] = tdItem.fieldType;
            fieldAr[index] = tdItem.fieldName;
            index++;
        }
        for(TDItem tdItem : td2.tdItemList){
            typeAr[index] = tdItem.fieldType;
            fieldAr[index] = tdItem.fieldName;
            index++;
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {//euqals方法，只要两个TupleDesc的属性数量相等，且td1的第i个属性类型==td2的第i个属性类型，则两个TupleDesc相等
        // some code goes here
        if(o == null)
            return false;
        if(!o.getClass().equals(TupleDesc.class))
            return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        if(tupleDesc.numFields() != this.numFields())
            return false;
        for(int i = 0;i < this.tdItemList.size(); i++){
            if(!tupleDesc.tdItemList.get(i).toString().equals(tdItemList.get(i).toString()))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {//返回TupleDesc的所有属性名和类型
        // some code goes here
        if(this.tdItemList.size() == 0) return "";
        String str = "";
        String tmp = "";
        for(TDItem tdItem : this.tdItemList){
            str += tdItem.toString();
        }
        return str;
    }
}
