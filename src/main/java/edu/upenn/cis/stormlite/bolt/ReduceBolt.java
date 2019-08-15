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
/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ReduceBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(ReduceBolt.class);

	
	Job reduceJob;

	/**
	 * This object can help determine when we have
	 * reached enough votes for EOS
	 */
	ConsensusTracker votesForEos;

	/**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEos = false;
	
	/**
	 * Buffer for state, by key
	 */
	Map<String, List<String>> stateByKey = new HashMap<>();

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    private DBWrapper Db;
    private Config config;
    private String location;
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
        this.config=(Config) stormConf;
        this.location=this.config.get("storagedirectory")+"/store_"+this.executorId+"_"+config.get("workerIndex");
        this.Db=new DBWrapper(this.location);
        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

        // TODO: determine how many EOS votes needed and set up ConsensusTracker (or however
        // you want to handle consensus)
        int nummap=Integer.valueOf(this.config.get("mapExecutors"));
        int numreduce=Integer.valueOf(this.config.get("reduceExecutors"));
        int workersize=WorkerHelper.getWorkers(this.config).length;
        neededVotesToComplete=(workersize-1)*nummap*numreduce+nummap;
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized boolean execute(Tuple input) {
        sentEos=(neededVotesToComplete==0);
    	if (sentEos) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
	        return false;
		} else if (input.isEndOfStream()) {
			neededVotesToComplete=neededVotesToComplete-1;
			if (neededVotesToComplete==0){
			    
			
    		log.debug("Processing EOS from " + input.getSourceExecutor());
			// : only if at EOS do we trigger the reduce operation over what's in BerkeleyDB for
    		// the associated key, and output all state
    		
    		// You may find votesForEos useful to determine when consensus is reacked
    		ArrayList<String> keys=this.Db.getkeys();
    		for (String key:keys){
    		    reduceJob.reduce(key,this.Db.getvalues(key).getvalues().iterator(),(Context) collector,executorId);
    		     WorkerPool.addkeyswritten(config);
    		}
    		WorkerPool.setstate(config,"waiting");
    		collector.emitEndOfStream(executorId);
    		sentEos = true;
		}


    	} else {
    		//  collect the tuples by key into BerkeleyDB (until EOS arrives, in the above condition)
    		log.debug("Processing " + input.toString() + " from " + input.getSourceExecutor());
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        Db.addkeyvalue(key,value);
	        WorkerPool.setstate(config,"reducing");
	        WorkerPool.addkeysread(config);
    		
    	}        
    	return true;
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
        this.Db.close();
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}

}
