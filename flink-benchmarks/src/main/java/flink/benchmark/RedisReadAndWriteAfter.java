package flink.benchmark;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.LOG;

public class RedisReadAndWriteAfter {
    private Jedis flush_jedis;
    private String keyToFlush;
    private String valueToFlush;
    HashMap<String, String> elemensTowrite;
    Long throughput=0L;


    public RedisReadAndWriteAfter(String redisServerName , int port) {
        flush_jedis=new Jedis(redisServerName,port);
    }

/*    public void execute(String key, String value) {


        synchronized(elemensTowrite) {
            elemensTowrite.put(key,value);
        }
    }*/
    public void execute( HashMap<String, String> elemensBatchTowrite) {

        synchronized(elemensTowrite) {
            elemensTowrite=elemensBatchTowrite;

        }
    }
    public void execute1(String id,String time) {

        synchronized(elemensTowrite) {
            elemensTowrite.put(id,time);

        }
        synchronized(throughput) {
            throughput++;

        }
    }
    public void executeForAgregate(String id,String time,String throughput_) {

        synchronized(elemensTowrite) {
            elemensTowrite.put(id,time);
            elemensTowrite.put(System.currentTimeMillis()+"","throughput:"+throughput_);

        }

    }


    public void write(String key,String field, String value) {
        //System.out.println("key1"+key+"  "+field+"  "+value);
        flush_jedis.hset(key,field,value);


    }

    public void prepare() {
        elemensTowrite=new HashMap<>();

        Runnable flusher = new Runnable() {
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(10000);
                        flushWindows();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted", e);
                }
            }
        };
        new Thread(flusher).start();
    }
    public void prepare_throuphput() {

        Runnable flusher = new Runnable() {
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        flushThrouphput();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted", e);
                }
            }
        };
        new Thread(flusher).start();
    }

    private void flushWindows() {
        synchronized (elemensTowrite) {
            for (String s : elemensTowrite.keySet()) {
                writeWindow(s, elemensTowrite.get(s));
                //writeWindow_Throughput(System.currentTimeMillis()+"",elemensTowrite.size()+""); //for non-aggregate
            }




            elemensTowrite.clear();
        }
    }

    private void flushThrouphput() {

        synchronized (throughput) {
            writeWindow_Throughput(System.currentTimeMillis()+"",throughput+"");
            throughput=0L;

        }
    }
    private void writeWindow(String key, String value) {
        String kv []=value.split(":");

        flush_jedis.hset(key, kv[0], kv[1]);
    }
    private void writeWindow_Throughput(String key, String value) {


        flush_jedis.hset(key, "throughput", value);
    }
}
