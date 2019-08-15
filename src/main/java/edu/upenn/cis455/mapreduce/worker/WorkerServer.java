package edu.upenn.cis455.mapreduce.worker;

import static spark.Spark.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.RunJobRoute;
import spark.Spark;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
    private String master;
    private String inputdir;
    private String outputdir;
    private String job;
	static Logger log = LogManager.getLogger(WorkerServer.class);
	
    static DistributedCluster cluster = new DistributedCluster();
    
    List<TopologyContext> contexts = new ArrayList<>();

	int myPort;
	
	static List<String> topologies = new ArrayList<>();
	
	public WorkerServer(int myPort, String master) throws MalformedURLException {
		this.myPort=myPort;
		//this.master=master;
		//this.inputdir=inputdir;
		//this.outputdir=outputdir;
		this.master=master;
		
		log.info("Creating server listener at socket " + myPort);
	    Worker worker=new Worker( myPort,master);
		port(myPort);
		WorkerPool.addworker(myPort,worker);
		worker.start();
		
    	final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        Spark.post("/definejob", (req, res) -> {
	        	
	        	WorkerJob workerJob;
				try {
					workerJob = om.readValue(req.body(), WorkerJob.class);
		        	
		        	try {
		        		log.info("Processing job definition request" + workerJob.getConfig().get("job") +
		        				" on machine " + workerJob.getConfig().get("workerIndex"));
						contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(), 
								workerJob.getTopology()));
						System.out.println("context added");
                        job=workerJob.getConfig().get("job");
                        worker.setjob(job);
						// Add a new topology
						synchronized (topologies) {
							topologies.add(workerJob.getConfig().get("job"));
						}
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					res.status(200);
					System.out.println("200, OK");
		            return "Job launched";
				} catch (IOException e) {
					e.printStackTrace();
					
					// Internal server error
					res.status(500);
					return e.getMessage();
				} 
				
	        	
        });
        Spark.get("/shutdown",(request,response)-> {
        	   response.status(200);
        		log.info("worker shutdown received");
        		WorkerServer.shutdown();
        		return "ShutDown";
        	
        });

        
        Spark.post("/runjob", new RunJobRoute(cluster));
        
        Spark.post("/pushdata/:stream", (req, res) -> {
				try {
					String stream = req.params(":stream");
					log.debug("Worker received: " + req.body());
					Tuple tuple = om.readValue(req.body(), Tuple.class);
					
					log.debug("Worker received: " + tuple + " for " + stream);
					
					// Find the destination stream and route to it
					StreamRouter router = cluster.getStreamRouter(stream);
					
					if (contexts.isEmpty())
						log.error("No topology context -- were we initialized??");
					
					TopologyContext ourContext = contexts.get(contexts.size() - 1);
					
					// Instrumentation for tracking progress
			    	if (!tuple.isEndOfStream())
			    		ourContext.incSendOutputs(router.getKey(tuple.getValues()));
			    		
			    	if (tuple.isEndOfStream())
			    	{
                            router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1),"");
                        }

                      else
                          {  router.executeLocally(tuple, contexts.get(contexts.size() - 1),"");
                      }

                    res.status(200);
			    	// : handle tuple vs end of stream for our *local nodes only*
			    	// Please look at StreamRouter and its methods (execute, executeEndOfStream, executeLocally, executeEndOfStreamLocally)
					
					return "OK";
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
					res.status(500);
					return e.getMessage();
				}
				
        });

	}
	
	public static void createWorker(Map<String, String> config) {
		if (!config.containsKey("workerList"))
			throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

		if (!config.containsKey("workerIndex"))
			throw new RuntimeException("Worker spout doesn't know its worker ID");
		else {
			String[] addresses = WorkerHelper.getWorkers(config);
			String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];

			log.debug("Initializing worker " + myAddress);

			URL url;
			try {
				url = new URL(myAddress);

				new WorkerServer(url.getPort(),config.get("master"));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void shutdown() {
		synchronized(topologies) {
			for (String topo: topologies)
				cluster.killTopology(topo);
		}
		
    	cluster.shutdown();
    	WorkerPool.shutdown();
    	Thread.interrupted();
	}

	/**
	 * Simple launch for worker server.  Note that you may want to change / replace
	 * most of this.
	 * 
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String args[]) throws MalformedURLException {
	    org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);
		if (args.length < 2) {
			System.out.println("Usage: WorkerServer [port number]");
			System.exit(1);
		}
		
		int myPort = Integer.valueOf(args[0]);
		String master=args[1];
		
		
		System.out.println("Worker node startup, on port " + myPort);
	
		WorkerServer worker = new WorkerServer(myPort,master);
		
		// TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce
		// here
	}
}
