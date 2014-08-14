package com.webonise.cacheservice.watchresource;

import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.webonise.cacheservice.CacheServiceUtils;
import com.webonise.cacheservice.RedisService;
import com.webonise.cacheservice.cache.EntriesCacheProcessor;
import com.webonise.cacheservice.cache.ResultsCacheProcessor;
import com.webonise.redis.model.User;

@Component
public class LocalCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalCacheManager.class);

    private static final String ENTRY_MODIFY = "ENTRY_MODIFY";

    private BlockingQueue<Path> dtoFilePathQueue = new LinkedBlockingQueue<>();

    @Value("${dir.basePath}")
    private String basePath;

    @Autowired
    private RedisService redisService;
    
    @Autowired
    private EntriesCacheProcessor entriesCacheProcessor;
    
    @Autowired
    private ResultsCacheProcessor resultsCacheProcessor;
    
    @PostConstruct
    public void build() {
        // Start populating data in Redis in a separate thread.
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
//                entriesCacheProcessor.buildCache();
//                resultsCacheProcessor.buildCache();
            }
        };
        new Thread(runnable).start();
    }
    
    @PostConstruct
    public void registerServiceWatch() {
        final ResourceWatcher watchService = new ResourceWatcher(basePath);
        IResourceObserver observer = new IResourceObserver() {
            @Override
            public void resourceModified(final String eventName, final Path filePath) {
                updateCache(eventName, filePath);
            }
        };

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                watchService.processEvents();
            }
        };
        watchService.addResourceObserver(observer);
        new Thread(runnable).start();
    }

    private void updateCache(String eventName, Path filePath) {
        switch (eventName) {
        case ENTRY_MODIFY:
            if ( dtoFilePathQueue.size() == 0 ) {
                dtoFilePathQueue.offer(filePath);
            } else if ( !dtoFilePathQueue.contains(filePath) ) {
                dtoFilePathQueue.offer(filePath);
            }

            if ( dtoFilePathQueue.size() > 0 ) {
                synchronized (dtoFilePathQueue) {
                    dtoFilePathQueue.notify();
                }
            }
            break;
        }
    }

    @PostConstruct
    public void processDTO_FileQueue() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CacheUpdater cacheUpdater = new CacheUpdater();
        executorService.execute(cacheUpdater);
        executorService.shutdownNow();

        try {
            executorService.awaitTermination(1, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
        }
    }

    class CacheUpdater extends Thread {
        @Override
        public void run() {
            while ( true ) {
                synchronized (dtoFilePathQueue) {
                    while ( dtoFilePathQueue.size() == 0 ) {
                        try {
                            dtoFilePathQueue.wait();
                        } catch (InterruptedException e) {
                            LOG.error("Inrerruption occured ");
                        }
                    }
                }
                Path filePath = dtoFilePathQueue.poll();
                updateCache(filePath);
            }
        }
    }

    private void updateCache(final Path filePath) {
        LOG.info("Got notification of file: {}", filePath.getFileName());
        boolean saved = false;
        User user = null;
        try {
            user = CacheServiceUtils.getUser(filePath);
            if ( user != null ) {
                saved = redisService.saveUser(user);
            }
        } catch (Exception e) {
           LOG.error("Error occured while saving User: {}", user.getKey());
        }
        if ( saved ) {
            LOG.debug("The User: {} is saved successfully", user.getKey());
        }

    }

}
