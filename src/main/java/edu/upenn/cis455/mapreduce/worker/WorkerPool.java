package edu.upenn.cis455.mapreduce.worker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import java.util.*;

public class WorkerPool{
    
    private static HashMap<Integer, Worker> workerpool=new HashMap<Integer,Worker>();
    
    public static HashMap<Integer, Worker> getworkerpool(){
        return workerpool;
    }
    
    public static void addworker(int port, Worker worker){
        workerpool.put(port, worker);
    }
    public static Worker getworker(Config config){
        int port=Worker.portfromconfig(config);
        return getworker(port);
    }
    
    public static Worker getworker(int port){
        if (!workerpool.containsKey(port)){
            return null;
        }
        return workerpool.get(port);
    }
    public static void shutdown(){
        for (int key:workerpool.keySet()){
            workerpool.get(key).shutdown();
        }
        workerpool.clear();
    }
    
    public static synchronized void setstate(Config config, String state){
        getworker(config).setstate(state);
        
    }
    public static synchronized void addkeysread(Config config){
         getworker(config).addkeysread();
    }
    public static synchronized void addkeyswritten(Config config){
         getworker(config).addkeyswritten();
    }
    public static synchronized void addresult(Config config,String result){
         getworker(config).addresult(result);
    }
    public static synchronized void setjob(Config config, String job){
        getworker(config).setjob(job);
    }
     public static synchronized void setmaster(Config config, String master){
        getworker(config).setmaster(master);
    }
    
    public static synchronized void setinputdir(Config config, String inputdir){
        getworker(config).setinputdir(inputdir);
        
    }
    
    public static synchronized void setoutputdir(Config config, String outputdir){
        getworker(config).setinputdir(outputdir);
        
    }
    
    
}