package at.bronzels.libcdcdw;

public abstract class DistLock {
    String lockUrl;
    String lockPath;

    public DistLock(String lockUrl, String lockPath) {
        this.lockUrl = lockUrl;
        this.lockPath = lockPath.replace(":", "_").replace(".", "_");
        //this.lockPath = lockPath;
    }

    abstract public void open();

    abstract public void close();

    abstract public void acquire(String subPath);

    abstract public void release(String subPath);

    public void acquire() {
        acquire(null);
    }

    public void release() {
        release(null);
    }

}
