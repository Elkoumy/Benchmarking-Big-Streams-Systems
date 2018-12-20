package flink.benchmark;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.LOG;

public class RedisReadAndWrite {
    private Jedis flush_jedis;
    private String keyToFlush;
    private String valueToFlush;

    public RedisReadAndWrite(String redisServerName , int port) {
        flush_jedis=new Jedis(redisServerName,port);
    }


    public void write(String key,String field, String value) {
        //System.out.println("key1"+key+"  "+field+"  "+value);
        flush_jedis.hset(key,field,value);


    }
    public void execute(String key, String value) {
        keyToFlush=key;
        valueToFlush=value;
    }

    public void prepare() {
        keyToFlush="";
        valueToFlush="";
//        Runnable flusher = new Runnable() {
//            public void run() {
//                try {
//                    while (true) {
//                        Thread.sleep(1000);
                        flushWindows();
//                    }
//                } catch (InterruptedException e) {
//                    LOG.error("Interrupted", e);
//                }
//            }
//        };
//        new Thread(flusher).start();
    }

    private void flushWindows() {
        //synchronized (keyToFlush) {
            writeWindow(keyToFlush, valueToFlush);
        //}
    }
    private void writeWindow(String key, String value) {

        flush_jedis.hset(key, "time_seen", value);
    }
}
