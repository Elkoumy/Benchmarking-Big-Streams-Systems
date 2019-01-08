package flink.benchmark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

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
//    HashMap<String, String> elemensTowrite_before;
    String throughput="";
    String totElements;
    Boolean flag;


    public RedisReadAndWriteAfter(String redisServerName , int port) {
        flush_jedis=new Jedis(redisServerName,port);

    }

    /*    public void execute(String key, String value) {


            synchronized(elemensTowrite) {
                elemensTowrite.put(key,value);
            }
        }*/
/*    public void execute_before( String id,String time) {

        synchronized(elemensTowrite_before) {
            elemensTowrite_before.put(id,time);

        }
    }*/

    public void execute1(String id,String time,String totlaElement) {

        synchronized(elemensTowrite) {
            elemensTowrite.put(id,time);
           // throughput++;

        }
/*        synchronized(totElements) {
            totElements=totlaElement;
            // throughput++;

        }*/


/*        synchronized(throughput) {
            throughput=throughput_+":"+throughput_Key;
        }*/
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
        throughput="";
        totElements="";

        Runnable flusher = new Runnable() {
            public void run() {
                try {
                    while (true) {
                        Thread.sleep(1000);
                        flushWindows();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted", e);
                }
            }
        };
        new Thread(flusher).start();
    }

 /*   public void prepare_before() {
        elemensTowrite_before=new HashMap<>();

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
    }*/
/*    public void prepare_throuphput() {

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
    }*/
/*    private void flushWindows_before() {
        Pipeline p = flush_jedis.pipelined();
        synchronized (elemensTowrite_before) {
            for (String s : elemensTowrite_before.keySet()) {
//                writeWindow(s, elemensTowrite_before.get(s));
                p.hset(s, "time_seen",elemensTowrite_before.get(s));

            }
            p.syncAndReturnAll();
            elemensTowrite_before.clear();
        }
    }*/
    private void flushWindows() {


/*        synchronized (totElements) {
            if(!totElements.equals("")){
                System.out.println(totElements);
                //flush_jedis.hset("tpt"+System.currentTimeMillis(),"throughput",totElements.toString());
            }

            totElements="";
        }*/
        synchronized (elemensTowrite) {
            Pipeline p = flush_jedis.pipelined();
//            if(elemensTowrite.size()>0){writeWindow_Throughput(System.currentTimeMillis()+"",elemensTowrite.size()+"");}

            for (String s : elemensTowrite.keySet()) {
                p.hset(s, "time_updated",elemensTowrite.get(s));
                //writeWindow(s, elemensTowrite.get(s));

            }
            p.hset("","","");
            p.sync();

            elemensTowrite.clear();
        }
/*        synchronized (throughput){
            if (throughput.length()>2) {
                String kv[] = throughput.split(":");

                if (Long.parseLong(kv[0]) > 0)
                    writeWindow_Throughput(kv[1] + "", kv[0] + ""); //for non-aggregate
            }
            throughput="";
        }*/
    }

/*    private void flushThrouphput() {

        synchronized (throughput) {
            writeWindow_Throughput(System.currentTimeMillis()+"",throughput+"");
            throughput="";

        }
    }*/
    private void writeWindow(String key, String value) {
        String kv []=value.split(":");
        flush_jedis.hset(key, kv[0], kv[1]);
    }
    private void writeWindow_Throughput(String key, String value) {
        flush_jedis.hset(key, "throughput", value);
    }
}
