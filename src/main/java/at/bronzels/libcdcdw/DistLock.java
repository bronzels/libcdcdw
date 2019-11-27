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

    abstract public void acquire();

    abstract public void release();

}
