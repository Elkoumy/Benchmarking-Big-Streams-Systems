package flink.benchmark;

import redis.clients.jedis.Jedis;

public class RedisReadAndWrite {
    private Jedis flush_jedis;

    public RedisReadAndWrite(String redisServerName , int port) {
        flush_jedis=new Jedis(redisServerName,port);
    }

 /*   public void prepare(){


    }*/
    public void write(String key,String field, String value) {
        //System.out.println("key1"+key+"  "+field+"  "+value);
        flush_jedis.hset(key,field,value);


    }
}
