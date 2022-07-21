package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */

/**
 * After you have implemented HeapPage, you will write methods for HeapFile in this lab to calculate
 * the number of pages in a file and to read a page from the file. You will then be able to fetch tuples from
 * a file stored on disk.
 */
public class HeapFile implements DbFile {
    //java的File类：代表磁盘实际存在的文件和目录
    private File file;
    private TupleDesc tupleDesc;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //上文提示：We suggest hashing the absolute file name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        /**
         * DbFile.java.readPage：
         * @throws IllegalArgumentException if the page,xx, does not exist in this file.
         */
        // some code goes here
        byte[] data=new byte[BufferPool.getPageSize()];
        HeapPage heapPage=null;//最后返回的page

        int tableId= pid.getTableId();
        int pgNo=pid.getPageNumber();

        //定义随机访问文件的类对象
        RandomAccessFile randomAccessFile=null;

        try {
            //定义随机访问文件的类对象
            randomAccessFile = new RandomAccessFile(file, "r");
            int pos = pid.getPageNumber() * BufferPool.getPageSize();//计算偏移量
            randomAccessFile.seek(pos);
            randomAccessFile.read(data, 0, data.length);
            heapPage=new HeapPage((HeapPageId) pid,data);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        //把page中的数据写入data，然后把data写入file
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    /*public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();//pageList用于存储所有脏页
        for (int i = 0; i < numPages(); i++) {
            //this.getId()=tableId, i=pgNo
            PageId pid = new HeapPageId(this.getId(),i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid,Permissions.READ_WRITE);
            if (p.getNumEmptySlots() == 0){
                Database.getBufferPool().releasePage(tid,pid);
                continue;
            }
            //找到有emptuSlots的page插入tuple
            p.insertTuple(t);//使用HeapFile的insertTuple
            p.markDirty(true,tid);
            pageList.add(p);
            return pageList;
        }

        //至此都没有找到空页
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file,true));
        byte[] emptyData = HeapPage.createEmptyPageData();//新建一个空页
        bw.write(emptyData);
        bw.close();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                new HeapPageId(getId(),numPages() - 1),Permissions.READ_WRITE);
        //由于file.length已经改变，故直接写numPages()-1就是最后一页
        p.insertTuple(t);
        p.markDirty(true,tid);
        pageList.add(p);
        return pageList;
    }*/

    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage page  = null;

        // find a non full page
        for(int i=0;i<numPages();++i){
            HeapPageId pid = new HeapPageId(getId(),i);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            if(page.getNumEmptySlots()!=0){
                break;
            }
            else{
                Database.getBufferPool().releasePage(tid,pid);
            }
        }

        // if not exist an empty slot, create a new page to store
        if(page == null || page.getNumEmptySlots() == 0){
            HeapPageId pid = new HeapPageId(getId(),numPages());
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(pid,data);
            writePage(heapPage);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        }

        page.insertTuple(t);

        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // delete tuple and mark page as dirty
        HeapPage page =  (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        page.deleteTuple(t);

        // return res
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }
    // see DbFile.java for javadocs
    /*public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                t.getRecordId().getPageId(),Permissions.READ_WRITE);
        p.deleteTuple(t);
        p.markDirty(true,tid);
        pageList.add(p);
        return pageList;
    }*/

    // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //iterate through the tuples of each page in the HeapFile
        //use the `BufferPool.getPage()`
        return new HeapFileIterator(this,tid);
    }

    //自定义HeapFileIterator类
    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;//用于遍历tuple
        private int whichPage;

        //构造函数
        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }

        //根据DbFileIterator提供给的接口来写函数
        @Override
        public void open() throws DbException, TransactionAbortedException {
            //打开iterator，加载第一页的tuples，令whichpage=0
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException{
            // The iterator must use the `BufferPool.getPage()` method
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null){
                return false;
            }

            //当前页tuple遍历完了
            if(!it.hasNext()){
                //看下一页还有没有hasNext
                if(whichPage < (heapFile.numPages()-1)){
                    whichPage++;
                    it = getPageTuples(whichPage);
                    return it.hasNext();
                }
                else{
                    return false;
                }
            }
            //当前页还有tuple未遍历，说明hasNext
            else{
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // 重新开一遍
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }

}
