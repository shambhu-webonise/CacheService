package com.webonise.cacheservice.cache;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;
import org.springframework.stereotype.Component;

import com.webonise.redis.model.User;

import drf.common.wrappers.cache.Cachable;

@Component
public class CacheServiceClient {

    private static final Logger LOG = LoggerFactory.getLogger(CacheServiceClient.class);

    @Autowired
    private RedisTemplate<String, Cachable> redisTemplate;

    private RedisAtomicInteger redisAtomicInteger;

    public void save(final Cachable cachable) {
        redisTemplate.opsForHash().put(cachable.getObjectKey(), cachable.getKey(), cachable);
    }

    public Map<Object, Object> fetchObjectList(Cachable cachable) {
        return redisTemplate.opsForHash().entries(cachable.getObjectKey());
    }

    public void deleteAll(Cachable cachable) {
        LOG.debug("Deleting all keys having Object_key: {}", cachable.getObjectKey());
        redisTemplate.opsForHash().delete(cachable.getObjectKey());
    }

    public Object read(Cachable cachable) {
        Object obj = redisTemplate.opsForHash().get(cachable.getObjectKey(), cachable.getKey());
        if ( obj == null ) {
            return cachable;
        }
        return obj;
    }

    public Object readUser(Cachable cachable) {
        return redisTemplate.opsForHash().get(cachable.getObjectKey(), cachable.getKey());
    }

    public long size(Cachable cachable) {
        return redisTemplate.opsForHash().size(cachable.getObjectKey());
    }

    public Object CASUpdate(final Cachable cachable) {
        return redisTemplate.execute(new SessionCallback<List<Object>>() {
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                operations.watch(cachable.getKey());
                User u = (User) operations.opsForHash().get(cachable.getObjectKey(), cachable.getKey());
                if ( ((User) cachable).getEmail() == null || !((User) cachable).getEmail().isEmpty() ) {
                    u.setEmail(((User) cachable).getEmail());
                }
                if ( ((User) cachable).getName() == null || !((User) cachable).getName().isEmpty() ) {
                    u.setName(((User) cachable).getName());
                }
                operations.multi();
                // operations.opsForSet().add("key", "value1");
                operations.opsForHash().put(u.getObjectKey(), u.getKey(), u);
                // This will contain the results of all ops in the transaction
                return operations.exec();
            }
        });

    }
    
    public Object getAndSet(SessionCallback callback) {
//        redisTemplate.setEnableTransactionSupport(true);
        return redisTemplate.execute(callback);
    }

    @SuppressWarnings({ "unchecked", "unchecked" })
    public Object getAndSet(final User user) {
        LOG.debug("Set and Get -- {}", user);
        redisTemplate.setEnableTransactionSupport(true);
        
        @SuppressWarnings({ "rawtypes" })
        SessionCallback<List<Object>> sessionCallback = new SessionCallback<List<Object>>() {
            @Override
            public List<Object> execute(RedisOperations operations) throws DataAccessException {
                LOG.debug("Set and Get -- inside execute -- {}", user);
                operations.watch( user.getObjectKey() + user.getKey());
                User preu = (User) operations.opsForHash().get(user.getObjectKey(), user.getKey());
                LOG.debug("{}", preu);
                
                User u = (User) operations.opsForHash().get(user.getObjectKey(), user.getKey());
                LOG.debug("{}", u);
                if ( user.getEmail() == null || !user.getEmail().isEmpty() ) {
//                    u.setEmail(user.getEmail());
                    if ( user.getEmail() == null || !user.getEmail().isEmpty() ) {
                        int a = Integer.parseInt(u.getEmail());
                        int b = Integer.parseInt(user.getEmail());
                        u.setEmail(a + b + "");
                    }
                    
                }
                if ( user.getName() == null || !user.getName().isEmpty() ) {
                    u.setName(user.getName());
                }
                operations.multi();
                
//                User u2 = (User) operations.opsForHash().get(user.getObjectKey(), user.getKey());
//                LOG.debug("{}", u2);
                
                operations.opsForHash().put( u.getObjectKey(), u.getKey(), u);
//                
//                User u3 = (User) operations.opsForHash().get(user.getObjectKey(), user.getKey());
//                LOG.debug("{}", u3);
                
                List<Object> objects = operations.exec();
                
                for ( Object object : objects ) {
                    LOG.debug("{}", object);
                    if ( object instanceof Boolean ) {
                        LOG.debug("Object retured is instanace of boolean------------");
                    }
                }
                return objects;
            }
        };
        return redisTemplate.execute(sessionCallback);
    }
}
