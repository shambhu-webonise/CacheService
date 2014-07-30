package com.webonise.cacheservice.watchresource;

import java.nio.file.Path;

public interface IResourceObserver {
    void resourceModified(String eventName, Path filePath);
}
