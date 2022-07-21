package simpledb;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    private PageId pid;
    private int tupleno;
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {//返回元组序号
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {//返回元组所属页的pageId
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {//判断相等
        // some code goes here
        if(o == null || o.getClass() != RecordId.class) return false;
        RecordId recordId = (RecordId) o;
        return(recordId.tupleno == this.tupleno && recordId.pid.equals(this.pid));
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode(){//返回hashcode
        // some code goes here
        //实现两个equal的recordId的hashcode也相等
        //3个条件：tableId、pageno和tupleno相等
        //getTableId和getPageNumber参见HeapPageId.java
        //包含关系：table->page->tuple
        //pid由tableid和pgno共同决定
        String s = "" + pid.getTableId() + pid.getPageNumber() + tupleno;
        return s.hashCode();
    }

}