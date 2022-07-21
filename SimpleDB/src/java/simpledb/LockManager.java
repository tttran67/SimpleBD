package simpledb;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

//自定义LockManager来管理锁
public class LockManager {

    //using ConcurrentHashMap because it is thread-safe
    //dependenciesSet存储当前transaction的依赖关系
    private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> dependenciesSet;
    //pageLocks关联page和它所有的锁
    private ConcurrentHashMap<PageId,LinkedList<PageLock>> pageLocks;

    public LockManager() {
        dependenciesSet = new ConcurrentHashMap<TransactionId, HashSet<TransactionId>>();
        pageLocks = new ConcurrentHashMap<PageId, LinkedList<PageLock>>();
    }

    /**
     * a specific transaction acquires the lock on the specific page
     * @param tid transaction id
     * @param pid page id
     * @param permissions the permission of the transaction on the page
     */
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions permissions){
        // if the permission is READ_ONLY, then acquire the SHARED_LOCK
        if(permissions.equals(Permissions.READ_ONLY)){
            return acquireSharedLock(tid, pid);
        }
        // else the permission is READ_WRITE, then acquire the EXCLUSIVE_LOCK
        else{
            return acquireExclusiveLock(tid, pid);
        }
    }

    /**
     * a specific transaction acquires the SHARED_LOCK on the specific page
     * @param tid transaction id
     * @param pid page id
     * @return true if the transaction acquires the lock successfully
     */
    //1.要加的锁是读锁
    private synchronized boolean acquireSharedLock(TransactionId tid, PageId pid) {
        // locklist存储pid的所有lock
        LinkedList<PageLock> lockList = pageLocks.get(pid);
        // 1-1.pid上没有锁->直接加锁
        if(lockList==null){
            lockList = new LinkedList<PageLock>();
        }
        //1-2.pid上有锁
        if(lockList.size() > 0){
            // 1-2-1.pid上只有1个锁
            if(lockList.size() == 1){
                // 1-2-1-1.是同一个事务
                if(lockList.getFirst().getTid().equals(tid)){
                    //                    // improve the lock type of the target transaction upto the EXCLUSIVE_LOCK
//                    lockList.removeFirst();
//                    // add a EXCLUSIVE_LOCK lock to the locks list
//                    addLock(tid,pid,Permissions.READ_WRITE);
//                    return true;
                    // 1-2-1-1-1.已有的锁是读锁->已经加锁，直接返回
                    if(lockList.getFirst().getType().equals(PageLock.SHARED_LOCK)){
                        return true;
                    }else{
                        // 1-2-1-1-1.已有的锁是写锁->加锁
                        addLock(tid,pid,Permissions.READ_ONLY);
                        return true;
                    }
                }
                // 1-2-1-2.不是同一个事务
                else{
                    // 1-2-1-2-1.已有的锁是读锁->加锁
                    if(lockList.getFirst().getType().equals(PageLock.SHARED_LOCK)){
                        addLock(tid,pid,Permissions.READ_ONLY);
                        return true;
                    }
                    // 1-2-1-2-1.已有的锁是写锁->加锁失败
                    else{
                        addDependency(tid, lockList.getFirst().getTid());
                        return false;
                    }
                }
            }
            // 1-2-2.pid上不止1个锁
            /*
                there are four cases:
                1. two locks belongs to the target transaction one SHARED_LOCK and one EXCLUSIVE_LOCK
                2. two locks belongs to other transaction one SHARED_LOCK and one EXCLUSIVE_LOCK
                3. many locks and one SHARED_LOCK belongs to the target transaction
                4. many locks and none belongs to the target transaction
                In this section only need to check if the lock is an EXCLUSIVE_LOCK and belongs to other transaction
            */
            else{
                for(PageLock lock:lockList){
                    // 1-2-2-1.当前lock是写锁
                    if(lock.getType().equals(PageLock.EXCLUSIVE_LOCK)){
                        // 1-2-2-1-1.写锁的tid不是当前tid->加锁失败
                        if(!lock.getTid().equals(tid)){
                            addDependency(tid, lock.getTid());
                            return false;
                        }else{
                            // 1-2-2-1-2.写锁的tid是当前tid->加锁
                            return true;
                        }
                    }
                    // 1-2-2-2.当前lock是读锁
                    else{
                        // 1-2-2-2-1.读锁的tid是当前tid->加锁
                        if(lock.getTid().equals(tid)){
                            return true;
                        }
                        //1-2-2-2-2.读锁的tid不是当前tid->继续遍历
                    }
                }
            }
        }
        //加锁
        addLock(tid, pid, Permissions.READ_ONLY);
        return true;
    }

    /**
     * a specific transaction acquires the EXCLUSIVE_LOCK on the specific page
     * @param tid transaction id
     * @param pid page id
     * @return true if the transaction acquires the lock successfully
     */
    //2.要加的锁是写锁
    private synchronized boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
        // locklist存储pid的所有lock
        LinkedList<PageLock> lockList = pageLocks.get(pid);
        // 2-1.pid上没有锁->直接加锁
        if(lockList==null){
            lockList = new LinkedList<PageLock>();
        }
        //2-2.pid上有锁
        if(lockList.size() > 0){
            // 2-2-1.pid上只有1个锁
            if(lockList.size() == 1){

                // 2-2-1-1.是同一个事务
                if(lockList.getFirst().getTid().equals(tid)){
                    //to be finished.这个if语句块里还没有完全搞清楚，周四学习了锁再来看
                    //to be finished.LockingTest有时过有时不过
                    // improve the lock type of the target transaction upto the EXCLUSIVE_LOCK
                    lockList.removeFirst();
                    // add a EXCLUSIVE_LOCK lock to the locks list
                    addLock(tid,pid,Permissions.READ_WRITE);
                    return true;
                    // 2-2-1-1-1.已有的锁是读锁->已经加锁，直接返回
//                    if(lockList.getFirst().getType().equals(PageLock.SHARED_LOCK)){
//                        return true;
//                    }else{
//                        addLock(tid,pid,Permissions.READ_ONLY);
//                        return true;
//                    }
                }
                // 2-2-1-2.不是同一个事务
                else{
                    // the only lock belongs to other transaction need to add the transaction to the dependenciesSet
                    addDependency(tid, lockList.getFirst().getTid());
                    return false;
                }
            }
            // 2-2-2.pid上不止1个锁
            else{
                /*
                there are four cases:
                1. two locks belongs to the target transaction one SHARED_LOCK and one EXCLUSIVE_LOCK
                2. two locks belongs to other transaction one SHARED_LOCK and one EXCLUSIVE_LOCK
                3. many locks and one SHARED_LOCK belongs to the target transaction
                4. many locks and none belongs to the target transaction
                 */
                // 2-2-2-1.pid有2个锁->可能成功可能失败
                // 2-2-2-2.pid不止2个锁->加锁失败
                if(lockList.size() == 2) {
                    for(PageLock lock : lockList){
                        // if there is an EXCLUSIVE_LOCK lock belongs to the transaction
                        if(lock.getTid().equals(tid) && lock.getType().equals(PageLock.EXCLUSIVE_LOCK)){
                            return true;
                        }
                    }
                    // add the dependency to the transaction
                    addDependency(tid, lockList.getFirst().getTid());
                }
                for(PageLock lock : lockList){
                    addDependency(tid, lock.getTid());
                }
                return false;
            }
        }
        else{
            addLock(tid,pid, Permissions.READ_WRITE);
            return true;
        }
    }

    /**
     * add lock to the lock list of the specific page
     * @param tid transaction id
     * @param pid page id
     * @param permission permission of the lock
     */
    private synchronized void addLock(TransactionId tid, PageId pid, Permissions permission) {
        PageLock lock = new PageLock(tid, permission);
        LinkedList<PageLock> lockList = pageLocks.get(pid);
        if(lockList==null){
            lockList = new LinkedList<PageLock>();
        }
        lockList.add(lock);
        pageLocks.put(pid, lockList);
        // when the lock can be added, it means that the transaction has no dependency
        deleteDependency(tid);
    }

    /**
     * add dependency to the transaction
     * @param tid transaction id
     * @param dependedTid transaction id that the transaction depends on
     */
    private synchronized void addDependency(TransactionId tid, TransactionId dependedTid) {
        if(tid.equals(dependedTid)){
            return;
        }
        HashSet<TransactionId> transactionIds = dependenciesSet.get(tid);
        if(transactionIds==null){
            transactionIds = new HashSet<TransactionId>();
        }
        transactionIds.add(dependedTid);
        dependenciesSet.put(tid, transactionIds);
    }

    /**
     * delete the dependency of the transaction
     * @param tid transaction id
     */
    private synchronized void deleteDependency(TransactionId tid) {
        dependenciesSet.remove(tid);
    }

    /**
     * release the lock on the specific page that belongs to the transaction
     * @param tid transaction id
     * @param pid page id
     */
    //事务执行完成，释放该事务在所有page上的所有锁
    public synchronized void releasePage(TransactionId tid, PageId pid){
        LinkedList<PageLock> lockList = pageLocks.get(pid);
        if(lockList==null || lockList.size() == 0){
            return;
        }
        for(PageLock lock : lockList){
            if(lock.getTid().equals(tid)){
                lockList.remove(lock);
                break;
            }
        }
        if(lockList.size() == 0){
            pageLocks.remove(pid);
        }
    }

    //获取tid在pid上的锁
    public synchronized PageLock getLock(TransactionId tid, PageId pid){
        LinkedList<PageLock> lockList = pageLocks.get(pid);
        if(lockList==null || lockList.size() == 0){
            return null;
        }
        for(PageLock lock : lockList){
            if(lock.getTid().equals(tid)){
                return lock;
            }
        }
        return null;
    }
    public synchronized boolean checkDeadLock(TransactionId tid){
        Set<TransactionId> diverseid=new HashSet<>();
        Queue<TransactionId> que=new ConcurrentLinkedQueue<>();
        que.add(tid);

        while(que.size()>0){
            TransactionId remove_tid=que.remove();
            if(diverseid.contains(remove_tid)) {
                continue;
            }
            diverseid.add(remove_tid);
            Set<TransactionId> now_set=dependenciesSet.get(remove_tid);
            if(now_set==null) {
                continue;
            }
            for(TransactionId now_tid:now_set){
                que.add(now_tid);
            }
        }

        ConcurrentHashMap<TransactionId,Integer> now_rudu=new ConcurrentHashMap<>();
        for(TransactionId now_tid:diverseid){
            now_rudu.put(now_tid,0);
        }
        for(TransactionId now_tid:diverseid){
            Set<TransactionId> now_set=dependenciesSet.get(now_tid);
            if(now_set==null) {
                continue;
            }
            for(TransactionId now2_tid:now_set){
                Integer temp = now_rudu.get(now2_tid);
                temp++;
                now_rudu.put(now2_tid,temp);
            }
        }

        while(true){
            int cnt=0;
            for(TransactionId now_tid:diverseid){
                if(now_rudu.get(now_tid)==null) {
                    continue;
                }
                if(now_rudu.get(now_tid)==0){
                    Set<TransactionId> now_set=dependenciesSet.get(now_tid);
                    if(now_set==null) {
                        continue;
                    }
                    for(TransactionId now2_tid:now_set){
                        Integer temp = now_rudu.get(now2_tid);
                        if(temp==null) {
                            continue;
                        }
                        temp--;
                        now_rudu.put(now2_tid,temp);
                    }
                    now_rudu.remove(now_tid);
                    cnt++;
                }
            }
            if(cnt==0) {
                break;
            }
        }

        if(now_rudu.size()==0) {
            return false;
        }
        return true;
    }
}