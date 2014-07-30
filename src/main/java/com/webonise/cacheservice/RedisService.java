package com.webonise.cacheservice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.stereotype.Component;

import com.webonise.redis.model.User;

@Component
public class RedisService {

    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);

    @Autowired
    private RedisTemplate<String, User> redisTemplate;
    
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private DefaultRedisMap<String, User> userMapTemplate;

    @Resource(name = "redisTemplate")
    private ListOperations<String, User> listOps;

    public boolean saveUser(User user) {

        redisTemplate.opsForHash().put(user.getObjectKey(), user.getKey(), user);
        // userMapTemplate.put(user.getKey(), user);

        return true;
    }

    public List<User> getDataList() {

        // return (Map<String, User>) userMapTemplate;
        List<User> userList = new ArrayList<User>();

        Map<Object, Object> map = redisTemplate.opsForHash().entries("USER");
        
        for ( Entry<Object, Object> object : map.entrySet() ) {
            userList.add((User) object.getValue());
        }
        saveAsList(userList);
        return userList;
    }

    private void saveAsList(List<User> userList) {
//       redisTemplate.opsForValue().set("LIST", userList.get(0));
        
    }

    public void deleteAll() {
        // redisTemplate.multi();
        redisTemplate.delete("USER");
    }
    
    public User getUser(String key) {
        User user = new User(key);
        user = (User) redisTemplate.opsForHash().get(user.getObjectKey(), user.getKey());
        return user;        
    }
}
