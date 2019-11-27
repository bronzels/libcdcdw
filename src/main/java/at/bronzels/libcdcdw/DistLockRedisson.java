package at.bronzels.libcdcdw;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

public class DistLockRedisson extends DistLock {
    RedissonClient redisson;

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

    public void acquire() {
        RLock lock = redisson.getLock(lockPath);
        lock.lock(1L, TimeUnit.MINUTES);
    }

    public void release() {
        RLock lock = redisson.getLock(lockPath);
        lock.unlock();
    }

}
