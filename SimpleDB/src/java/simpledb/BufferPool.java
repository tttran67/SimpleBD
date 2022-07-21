package simpledb;
import java.util.*;
import java.io.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static java.lang.Thread.currentThread;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;//默认page大小
    private final int numPages;//bufferpool能读取的最大page数
    private ConcurrentHashMap<PageId,Page> pid2pages;
    private final ConcurrentHashMap<PageId,Integer> pageAge;
    //pageAge将pid映射到该页对应的age
    private int age;
    private PageLockManager lockManager;

    //自定义锁类
    private class Lock{
        TransactionId tid;
        int lockType;   // 0：读锁，1：写锁

        public Lock(TransactionId tid,int lockType){
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    //自定义PageLockManager类实现对锁的管理
    //功能：申请锁、释放锁、查看指定数据页的指定事务是否有锁
    private class PageLockManager{
        //lockMap保存对应page上的所有锁，如果page上没有锁则将其移出lockMap
        ConcurrentHashMap<PageId,Vector<Lock>> lockMap;

        public PageLockManager(){
            lockMap = new ConcurrentHashMap<PageId,Vector<Lock>>();
        }

        //申请锁
        public synchronized boolean acquireLock(PageId pid,TransactionId tid,int lockType){
            // 1.页面没有锁->直接加锁
            if(lockMap.get(pid) == null){
                //根据tid和locktype新建锁
                Lock lock = new Lock(tid,lockType);
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                lockMap.put(pid,locks);
                return true;
            }
            //2.page上有锁->2种情况
            Vector<Lock> locks = lockMap.get(pid);

            //2-1.tid在pid上有锁
            for(Lock lock:locks){
                //遍历现有的锁，找到在该事务上已经有的锁
                //对应四种情况：
                //1:想要shared，已有ex；
                //2:想要ex，已有shared；
                //3:想要ex，已有ex
                //4:想要shared，已有shared
                if(lock.tid == tid){
                    // 已经有同样的锁 -> 成功
                    if(lock.lockType == lockType)
                        return true;
                    // 已经有写锁 -> 不需要再上读锁，成功
                    if(lock.lockType == 1)
                        return true;
                    // 已经有读锁 -> 升级锁，成功
                    if(locks.size()==1){
                        lock.lockType = 1;
                        return true;
                    }else{
                        //对应情况2，不止一个shared锁，则不能上ex锁 -> 失败
                        return false;
                    }
                }
            }
            //2-2.tid在pid上无锁 -> 看其他t在pid上的锁
            //2-2-1.已有的锁是写锁 -> 无法加锁
            if (locks.get(0).lockType ==1){
                assert locks.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }

            // 2-2-2.要上的锁是读锁 -> 加锁
            if(lockType == 0){
                Lock lock = new Lock(tid,0);
                locks.add(lock);
                lockMap.put(pid,locks);

                return true;
            }
            // 2-2-3.已有的锁是读锁且要上的锁是写锁
            return false;
        }
        //释放锁
        public synchronized boolean releaseLock(PageId pid,TransactionId tid){
            //1.pid上没有锁
            assert lockMap.get(pid) != null : "page not locked!";
            //2.pid上有锁 -> pid上是否有tid的锁
            Vector<Lock> locks = lockMap.get(pid);
            for(int i=0;i<locks.size();++i){
                Lock lock = locks.get(i);
                //pid上有tid的锁
                if(lock.tid == tid){
                    locks.remove(lock);
                    //如果pid上没有锁了，就将它从lockMap上移除
                    if(locks.size() == 0)
                        lockMap.remove(pid);
                    return true;
                }
            }
            //pid上没有tid的锁
            return false;
        }
        //查看指定事务是否在指定页上锁
        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            //1.pid上没有锁
            if(lockMap.get(pid) == null)
                return false;
            //2.pid上有锁
            Vector<Lock> locks = lockMap.get(pid);
            //遍历pid
            for(Lock lock:locks){
                if(lock.tid == tid){
                    return true;
                }
            }
            return false;
        }
    }
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages=numPages;
        pid2pages=new ConcurrentHashMap<>(this.numPages);
        pageAge = new ConcurrentHashMap<PageId,Integer>();
        age = 0;
        lockManager = new PageLockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        //先给该page在事务tid上加锁
        long start = System.currentTimeMillis();//获取开始时间
        long timeout = new Random().nextInt(2000) + 1000;
        boolean isLocked = false;//定义一个bool变量来看是否上好锁了，用于防止死锁！
        while(!isLocked)
        {
            long now = System.currentTimeMillis();//获取当前时间

            if(now-start > timeout){
                //如果超时，则判断死锁，事务中断
                throw new TransactionAbortedException();
            }
            //若还未给page上tid的锁，则给他上锁
            if(perm == Permissions.READ_ONLY)
                //读锁
                isLocked = lockManager.acquireLock(pid,tid,0);
            else
                //写锁
                isLocked = lockManager.acquireLock(pid,tid,1);
        }
        //如果这个page已经在缓存里，直接返回即可
        if(pid2pages.containsKey(pid)){
            return pid2pages.get(pid);
        }
        //如果这个page不在缓存里，就把page放进缓存
        else{
            //如果page数已满了，则要先赶出去一个页面，才能再新加进去
            if(pid2pages.size()>=numPages){
                evictPage();
            }
            //通过pid（PageId）找到这个page
            DbFile dbFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page=dbFile.readPage(pid);
            //把page存到buffpool里
            pid2pages.put(pid,page);
            pageAge.put(pid,age++);
        }
        return pid2pages.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    //释放tid在pid上的锁
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    //事务完成后的处理
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    //判断tid在p上是否有锁
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p,tid);
    }
    //重新加载被tid污染的pages
    private synchronized void restorePages(TransactionId tid) {
        for (PageId pid : pid2pages.keySet()) {
            Page page = pid2pages.get(pid);
            if (page.isDirty() == tid) {
                int tabId = pid.getTableId();
                DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                Page pageFromDisk = file.readPage(pid);

                pid2pages.put(pid, pageFromDisk);
            }
        }
    }
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            //如果事务提交了，则将所有page写入磁盘
            flushPages(tid);
        }else{
            //未提交则从磁盘重新取page
            restorePages(tid);
        }
        //释放tid在所有page上的锁
        for(PageId pid:pid2pages.keySet()){
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
        }
    }
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        //传入insert后的page列表，作为更新参数
        updateBufferPool(f.insertTuple(tid,t),tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        //传入delete后的page列表，作为更新参数
        updateBufferPool(f.deleteTuple(tid,t),tid);
    }

    //自定义updateBufferPool函数
    private void updateBufferPool(ArrayList<Page> pagelist,TransactionId tid) throws DbException{
        for(Page p:pagelist){
            p.markDirty(true,tid);
            //如果当前bufferpool存不下了，就执行evict
            if(pid2pages.size() > numPages)
                evictPage();
            //加入p
            pid2pages.put(p.getId(),p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        //flashAllPages循环调用flushPage
        for(Page p:pid2pages.values()){
            flushPage(p.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        //将Page移出bufferPool
        pid2pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //将脏页写入磁盘并标记为非脏
        Page p = pid2pages.get(pid);
        TransactionId tid = null;
        // flush it if it is dirty
        tid = p.isDirty();
        if(tid != null){
            //isDirty函数：Get the id of the transaction that last dirtied this page, or null if the page is clean
            // write to disk
            //写日志（事务，更新前，更新后）
            Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
            Database.getLogFile().force();
            // 将page写到disk里
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            //取消标记脏页
            p.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Page page: pid2pages.values()){
            if(page.isDirty() != null && page.isDirty() == tid){
                flushPage(page.getId());
            }
        }
        /*for (PageId pid : pid2pages.keySet()) {
            Page page = pid2pages.get(pid);
            if (page.isDirty() == tid) {
                flushPage(pid);
            }
        }*/
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        assert numPages == pid2pages.size() : "Buffor Pool is not full, not need to evict page";
        PageId pageId = null;
        int oldestAge = -1;
        //找到oldest的非脏页驱逐
        for (PageId pid: pageAge.keySet()) {
            Page page = pid2pages.get(pid);
            //不能驱逐脏页
            if (page.isDirty() != null)
                continue;
            if (pageId == null) {
                pageId = pid;
                oldestAge = pageAge.get(pid);
                continue;
            }
            if (pageAge.get(pid) < oldestAge) {
                pageId = pid;
                oldestAge = pageAge.get(pid);
            }
        }
        //所有页都脏
        if (pageId == null)
            throw  new DbException("failed to evict page: all pages are either dirty");
        Page page = pid2pages.get(pageId);
        //驱逐oldest的非脏页
        pid2pages.remove(pageId);
        pageAge.remove(pageId);
    }

}

