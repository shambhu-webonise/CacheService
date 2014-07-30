package com.webonise.cacheservice.watchresource;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ResourceWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceWatcher.class);
    
    private WatchService watcher;
    private Map<WatchKey, Path> keys;
    private List<IResourceObserver> resourceObservers = new ArrayList<IResourceObserver>(3);
    
    public ResourceWatcher(String resource) {
        try {
            watcher =  FileSystems.getDefault().newWatchService();
            keys = new HashMap<WatchKey, Path>();
            
            File resourceFile = new File(resource);
            if ( resourceFile.isDirectory() ) {
                register(resourceFile.toPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_MODIFY);
        keys.put(key, dir);
    }
    
    public void processEvents() {
        LOG.info("Process Events -- ");
        while ( true ) {
            WatchKey key;
            try {
                key = watcher.take();
                LOG.debug("Process on Key -- {} ", key.toString());
            } catch (InterruptedException x) {
                return;
            }
            Path dir = keys.get(key);

            if ( dir == null ) {
                continue;
            }

            for ( WatchEvent<?> event : key.pollEvents() ) {
                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);

                Path name = ev.context();
                if ( name == null ) {
                    continue;
                }

                Path filePath = dir.resolve(name);
                LOG.info("Got WatchService notification :: {} :: for file name {}", event.kind().name(), filePath);
                notifyResourceUpdated(event.kind().name(), filePath);
                break;
            }
            boolean valid = key.reset();
            if ( !valid ) {
                keys.remove(key);

                // All directories are inaccessible
                if ( keys.isEmpty() ) {
                    break;
                }
            }
        }
    }
    
    private void notifyResourceUpdated(String eventName, Path filePath) {
        for ( Iterator<IResourceObserver> iterator = resourceObservers.iterator(); iterator.hasNext(); ) {
            IResourceObserver observer = iterator.next();
            observer.resourceModified(eventName, filePath);
        }
    }
    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }
    
    public void addResourceObserver(IResourceObserver observer) {
        resourceObservers.add(observer);
    }
}
