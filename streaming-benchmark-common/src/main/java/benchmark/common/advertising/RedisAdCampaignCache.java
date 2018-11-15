/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package benchmark.common.advertising;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.security.auth.login.Configuration;
import java.util.HashMap;

public class RedisAdCampaignCache {
    private Jedis jedis;
    private JedisPool jedisPool;
    private HashMap<String, String> ad_to_campaign;
    private final int HOUR_IN_SECONDS = 3600 * 10;  // 10 HOURS


    public RedisAdCampaignCache(String redisServerHostname) {
//        jedis = new Jedis(redisServerHostname);
//        this.config = config;
        jedisPool = new JedisPool(buildJedisPoolConfiguration(), redisServerHostname, 6379, HOUR_IN_SECONDS, null);
        jedis = jedisPool.getResource();
    }

    public void prepare() {
        ad_to_campaign = new HashMap<String, String>();
    }

    private JedisPoolConfig buildJedisPoolConfiguration() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(1000);
        return jedisPoolConfig;
    }
    public String execute(String ad_id) {
        String campaign_id = ad_to_campaign.get(ad_id);
        if(campaign_id == null) {
            campaign_id = jedis.get(ad_id);
            if(campaign_id == null) {
                return null;
            }
            else {
                ad_to_campaign.put(ad_id, campaign_id);
            }
        }
        return campaign_id;
    }
}
