package com.webonise.cacheservice;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.stereotype.Component;

import com.webonise.cacheservice.cache.CacheServiceClient;
import com.webonise.redis.model.User;

import drf.common.wrappers.cache.Cachable;

@Component
public class RedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisService.class);

    @Autowired
    private CacheServiceClient cacheClient;

    @Autowired
    private RedisTemplate<String, User> redisTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private DefaultRedisMap<String, User> userMapTemplate;

    @Resource(name = "redisTemplate")
    private ListOperations<String, User> listOps;

    public boolean saveUser(User user) {

        User u = (User) cacheClient.readUser(user);
        if ( u != null ) {
            boolean isCased = false;
            do {
                
//                Object object = cacheClient.getAndSet(getCallback(user));
                Object object = cacheClient.getAndSet(user);
                LOG.debug("{}", object);
                if ( object != null ) {
                    isCased = true;
                }
            } while ( !isCased );
        } else {
            cacheClient.save(user);
        }
        return true;
    }

    @SuppressWarnings("rawtypes")
    private SessionCallback getCallback(final User user) {
        SessionCallback sessionCallback = new SessionCallback() {
            @Override
            @SuppressWarnings({"unchecked" })
            public Object execute(RedisOperations operations) throws DataAccessException {
                Object b = null;
                User preu = (User) operations.opsForHash().get(user.getObjectKey(), user.getKey());
                LOG.debug("{}", preu);
                operations.watch( user.getKey());
                operations.multi();
                
                User u = (User) operations.opsForHash().get(user.getObjectKey(), user.getKey());
                LOG.debug("{}", u);
                if ( user.getEmail() == null || !user.getEmail().isEmpty() ) {
                    u.setEmail(user.getEmail());
                }
                if ( user.getName() == null || !user.getName().isEmpty() ) {
                    u.setName(user.getName());
                }
                operations.opsForHash().put( u.getObjectKey(), u.getKey(), u);
                List<Object> objects = operations.exec();
                for ( Object object : objects ) {
                    LOG.debug("{}", object);
                    if ( object instanceof Boolean ) {
                        LOG.debug("Object retured is instanace of boolean------------");
                        if ( (boolean)object == true ) {
                            b = new Object();
                        }
                    }
                }
                return b;
            }
        };
        return sessionCallback;
    }

    public List<User> getDataList() {

        // return (Map<String, User>) userMapTemplate;
        List<User> userList = new ArrayList<User>();

        // Map<Object, Object> map = redisTemplate.opsForHash().entries("USER");
        Map<Object, Object> map = cacheClient.fetchObjectList(new User());

        for ( Entry<Object, Object> object : map.entrySet() ) {
            userList.add((User) object.getValue());
        }
        saveAsList(userList);
        return userList;
    }

    private void saveAsList(List<User> userList) {
        // redisTemplate.opsForValue().set("LIST", userList.get(0));

    }

    public void deleteAll() {
        // redisTemplate.multi();
        redisTemplate.delete("USER");
        // cacheClient.deleteAll(new User());
    }

    public User getUser(String key) {
        User user = new User(key);
        // user = (User) redisTemplate.opsForHash().get(user.getObjectKey(),
        // user.getKey());
        user = (User) cacheClient.read(user);
        return user;
    }
}
