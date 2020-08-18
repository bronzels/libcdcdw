package at.bronzels.libcdcdw;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

public class DistLockRedisson extends DistLock {
    RedissonClient redisson;
    RLock lock = null;

    public DistLockRedisson(String lockUrl, String lockPath) {
        super(lockUrl, lockPath);
    }

    public void open() {
        // 1. Create config object
        Config config = new Config();

        config.useSingleServer()
                .setAddress("redis://" + lockUrl)
        //.setConnectionMinimumIdleSize(1)
        ;

        // Sync and Async API
        redisson = Redisson.create(config);
    }

    public void close() {
        if (redisson != null)
            redisson.shutdown();
    }

    public void acquire(String subPath) {
        boolean isLock = false;
        while(!isLock) {
            lock = redisson.getLock(subPath == null ? lockPath : (lockPath + ":" + subPath));
            try {
                isLock = lock.tryLock(5, 5, TimeUnit.SECONDS);
            } catch (Exception e) {
            } finally {
                if(!isLock && lock.isLocked() && lock.isHeldByCurrentThread())
                    lock.unlock();
            }
        }
    }

    public void release(String subPath) {
        if(lock.isLocked() && lock.isHeldByCurrentThread())
            lock.unlock();
    }

}
