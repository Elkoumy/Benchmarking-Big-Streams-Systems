package flink.benchmark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.LOG;

public class RedisReadAndWriteBefore {
    private Jedis flush_jedis;

    private String keyToFlush;
    private String valueToFlush;
    HashMap<String, String> elemensTowrite_before;
    String throughput="";
    Boolean flag;
    String totElements="";


    public RedisReadAndWriteBefore(String redisServerName , int port) {
        flush_jedis=new Jedis(redisServerName,port);

    }


    public void execute_before( String id,String time,String totlaElement) {

        synchronized(elemensTowrite_before) {
            elemensTowrite_before.put(id,time);

        }
        synchronized(totElements) {
            totElements=totlaElement;

        }
    }




    public void prepare_before() {
        elemensTowrite_before=new HashMap<>();
        totElements="";

        Runnable flusher = new Runnable() {
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        flushWindows_before();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted", e);
                }
            }
        };
        new Thread(flusher).start();
    }

    private void flushWindows_before() {
        synchronized (totElements) {
            if(!totElements.equals("")){
                System.out.println(totElements+"*");
                flush_jedis.hset("tpt*"+System.currentTimeMillis(),"throughput*",totElements.toString());
            }

            totElements="";
        }

        synchronized (elemensTowrite_before) {
            Pipeline p = flush_jedis.pipelined();
            for (String s : elemensTowrite_before.keySet()) {
//                writeWindow(s, elemensTowrite_before.get(s));
                p.hset(s, "time_seen",elemensTowrite_before.get(s));

            }
            p.hset("","","");
            p.sync();

            elemensTowrite_before.clear();
        }
    }

}
