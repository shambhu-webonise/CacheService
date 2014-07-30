package com.webonise.cacheservice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Path;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.webonise.redis.model.User;

public class CacheServiceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CacheServiceUtils.class);

    private static Object readFile(String filePath) throws Exception {
        Object objectStream = null;
        ObjectInputStream ois = null;
        FileInputStream fin = null;
        if ( new File(filePath).exists() ) {
            try {
                if ( filePath != null ) {
                    fin = new FileInputStream(filePath);
                } else {
                    throw new Exception("Error occured while reading the serialised file");
                }
                if ( fin != null )
                    ois = new ObjectInputStream(fin);
                objectStream = SerializationUtils.deserialize(ois);
            } catch (IOException | ClassNotFoundException ex) {
                LOG.error("Error occured while reading the serialised file", ex);
                throw new Exception(ex.getMessage());
            }
        } else {
            LOG.error("file {} does not exist", filePath);
            // throw new ServiceException("File not Found");
        }
        return objectStream;
    }

    public static User getUser(Path filePath) {
        User user = null;
        try {
            user = (User) readFile(filePath.toAbsolutePath().toString());
        } catch (Exception e) {
            LOG.error("Error occured while reading file: {}", filePath);
        }
        return user;
    }

}
