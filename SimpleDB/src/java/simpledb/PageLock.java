package simpledb;
import simpledb.TransactionId;
import java.util.Objects;
//自定义锁类
public class PageLock {

    public static final String EXCLUSIVE_LOCK = "EXCLUSIVE_LOCK";
    public static final String SHARED_LOCK = "SHARED_LOCK";

    private TransactionId tid;
    private String lockType;

    public PageLock(TransactionId tid, Permissions lockType) {
        this.tid = tid;
        // if the transaction's permission is READ_WRITE, then the lock is EXCLUSIVE_LOCK
        if(lockType.equals(Permissions.READ_WRITE)) {
            this.lockType = EXCLUSIVE_LOCK;
        }
        // if the transaction's permission is READ_ONLY, then the lock is SHARED_LOCK
        else {
            this.lockType = SHARED_LOCK;
        }
    }

    public TransactionId getTid() {
        return tid;
    }

    public String getType() {
        return lockType;
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }
        if(obj==null||getClass()!=obj.getClass()){
            return false;
        }
        PageLock objLock=(PageLock) obj;
        return tid.equals(objLock.getTid())&&lockType.equals(objLock.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid, lockType);
    }
}