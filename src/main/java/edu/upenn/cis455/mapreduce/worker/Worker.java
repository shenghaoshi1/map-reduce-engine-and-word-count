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

public class Worker extends Thread{
    private int port;
    private String master="";
    private String state="idle"; // mapping, waiting, reducing or idle
    private int keysread=0;
    private int keyswritten=0;
    private ArrayList<String> results=new ArrayList<String>();
    private String inputdir="";
    private String outputdir="";
    private boolean isdone=false;
    private String job=null;
    static Logger logger = LogManager.getLogger(Worker.class);
    
    public static int portfromconfig(Config config){
        if (!config.containsKey("workerList"))
			throw new RuntimeException("Worker doesn't have list of worker IP addresses/ports");

		if (!config.containsKey("workerIndex"))
			throw new RuntimeException("Worker doesn't know its worker ID");
		else {
			String[] addresses = WorkerHelper.getWorkers(config);
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];
			try {
				URL url = new URL(myAddress);
				return url.getPort();
			}
			catch(Exception e){
			    logger.error("cannot get port from config");
			}
			

    }
    return -1;
}

public Worker(int port){
    this.port=port;
}
    public Worker(int port, String master){
        this.master=master;
        this.port=port;
    }
    
    public Worker(String master,int port, String inputdir, String outputdir){
        this.master=master;
        this.port=port;
        this.inputdir=inputdir;
        this.outputdir=outputdir;
        
    }
     public Worker(String master,int port,String job, String inputdir, String outputdir){
        this.master=master;
        this.port=port;
        this.inputdir=inputdir;
        this.outputdir=outputdir;
        this.job=job;
        
    }
    
    public String getjob(){
        return this.job;
    }
    public void setjob(String job){
        this.job=job;
    }
    @Override
    public void run(){
        while (!isdone){
            String uri="http://"+getmaster()+"/workerstatus";
            String params="port="+getport()+"&status="+getstate()+"&job="+getjob()+"&keysRead="+getkeysread()+"&keysWritten="+getkeyswritten()+"&results="+getresult();
            System.out.println("uri+params "+uri+params);
            logger.debug("start to report " +uri+params);
            
             URL url;
        try{
            url=new URL(uri+"?"+params);
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
			http.setRequestMethod("GET");
			http.getResponseMessage();
			http.disconnect();
			System.out.println("report success");
			logger.debug("report success");
        }
        catch (Exception e){
            logger.error("report error"+uri+params);
            System.out.println("report failue");
    }

        try{
            sleep(1000);
        }
        catch (Exception e){
            logger.error("sleep error");
            
        }
        
 
            
        }
       
            
            
        }
        
        
    
    public int getport(){
        return this.port;
    }
    
    public String getmaster(){
        return this.master;
    }
    
    public void setstate(String state){
        this.state=state;
        if (state.equals("idle")){
            System.out.println("setting idle");
            this.keysread=0;
            this.keyswritten=0;
            this.results=new ArrayList<String>();
        }
    }
    
    public String getstate(){
        return this.state;
    }
    
    public int getkeysread(){
        return this.keysread;
    }
    
    public int getkeyswritten(){
        return this.keyswritten;
    }
    
    public void addkeysread(){
        this.keysread++;
        
    }
    
    public void addkeyswritten(){
        this.keyswritten++;
    }
    
    public void addresult(String result){
       
            results.add(result);
       
    }
    public String getresult(){
        ArrayList<String> res= new ArrayList<String>();
        int count=0;
        for( String result:this.results){
           res.add(result);
            count=count+1;
            if (count==100){
                break;
            }
            
        }
        String output="is";
        for (String in:res){
            output=output+in+",";
            
        }
        return output;
    }
    public void shutdown(){
        this.isdone=true;
    }
    public void setinputdir(String inputdir){
        this.inputdir=inputdir;
    }
    public void setoutputdir(String outputdir){
        this.outputdir=outputdir;
    }
    public void setmaster(String master){
        this.master=master;
    }
    
    
    
    
}
