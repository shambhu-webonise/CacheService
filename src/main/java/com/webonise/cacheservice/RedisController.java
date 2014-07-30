package com.webonise.cacheservice;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.webonise.redis.model.User;

/**
 * Handles requests for the application home page.
 */
@Controller
public class RedisController {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisController.class);
	
	@Autowired
    private RedisService redisService;

    /**
     * Simply selects the home view to render by returning its name.
     */
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String home(Locale locale, Model model) {
        logger.info("index is called.");
        return "home";
    }

    @RequestMapping(value = "/addUser", method = RequestMethod.POST)
    public String saveInRedis(@RequestParam("key") String key, @RequestParam("value") String value, Model model) {
        logger.info("KEY: {}, VALUE: {}", key, value);
        if ( !key.isEmpty() && !value.isEmpty() ) {
            User user = new User(key, value);
            redisService.saveUser(user);
        }
        return "home";
    }

    @RequestMapping(value = "/getAllUser", method = RequestMethod.GET)
    public String getFromRedis(Model model) {
        logger.info("Fetching all data from redis");
        List<User> userList = redisService.getDataList();
        model.addAttribute("userList", userList);
        return "home";
    }

    @RequestMapping(value = "/deleteAllUser", method = RequestMethod.GET)
    public String deleteFromRedis(Model model) {
        logger.info("Deleting all data from redis");
        redisService.deleteAll();
        return "home";
    }
    
    @RequestMapping(value = "/getUser", method = RequestMethod.GET)
    public String getFromRedis(@RequestParam("key") String key, Model model) {
        logger.info("Fetching user having user key: {} data from redis", key);
        if ( !key.isEmpty() ) {
            User user = redisService.getUser(key);
            logger.info("User is {}", user);
            model.addAttribute("USER", user);
        }
        return "home";
    }
}
