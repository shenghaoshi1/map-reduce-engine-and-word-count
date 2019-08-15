package edu.upenn.cis.stormlite.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import edu.upenn.cis455.mapreduce.worker.WorkerPool;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.ConsensusTracker;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis455.mapreduce.Context;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
public class PrintBolt implements IRichBolt{
    
    
    static Logger log = LogManager.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();
	private Config config;
	private String outputdir;
	private FileWriter writer;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    int neededVotesToComplete;

	@Override
	public void cleanup() {
		
	}

	@Override
	public boolean execute(Tuple input) {
	    String key;
	    String value;
	   
	   
		if (!input.isEndOfStream()){
		    try{
	    key= input.getStringByField("key");
	     value= input.getStringByField("value");
    }
    catch( Exception e){
        return false;
    }

	    
	    String line=key+"_"+value;
			System.out.println(getExecutorId() + ": " + input.toString());
			try{
            writer=new FileWriter(outputdir + "output.txt",true); 
            BufferedWriter  out = new BufferedWriter(writer);
            out.write(line+"\r\n");
            out.close();
            log.info("print key:value "+line);
            WorkerPool.addresult(this.config,line);
        }
        catch(IOException e){
            log.error("unable to output");
        }
    }
    else{
        neededVotesToComplete--;
        if (neededVotesToComplete==0){
        WorkerPool.setstate(this.config,"idle");
        WorkerPool.setjob(this.config,"none");
    }

        
        
    }

        

			
		
		return true;
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
	    this.config=(Config) stormConf;
	    if (this.config.get("outputdirectory")!=null ||this.config.get("outputdirectory").equals("")){
	    outputdir=this.config.get("storagedirectory")+"/"+this.config.get("outputdirectory")+"/";}
	    else{
	        outputdir=this.config.get("storagedirectory")+"/";
	    }
	    File output=new File(outputdir);
	    if(!output.exists()){
			output.mkdir();
		}
		File outputfile = new File(outputdir + "output.txt");
		if(outputfile.exists()){
			outputfile.delete();
		}
		int reducerNum = Integer.parseInt(stormConf.get("reduceExecutors"));
		 neededVotesToComplete = reducerNum*WorkerHelper.getWorkers(stormConf).length;
	    
		
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}
}