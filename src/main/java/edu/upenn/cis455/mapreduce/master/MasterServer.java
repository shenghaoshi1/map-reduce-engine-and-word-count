package edu.upenn.cis455.mapreduce.master;
import java.util.*;
import static spark.Spark.*;
import java.io.*;
import java.net.URL;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.PrintBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.spout.WordFileSpout;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import java.net.HttpURLConnection;
import edu.upenn.cis.stormlite.tuple.Fields;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
public class MasterServer {
    //static Logger logger = LogManager.getLogger(MasterServer.class);
  static final long serialVersionUID = 455555001;
  static final int myPort = 8080;
  public static HashMap<String,HashMap<String,String>> workers=new HashMap<String,HashMap<String,String>>();
//  public static void registerStatusPage() {
//		get("/status", (request, response) -> {
//            response.type("text/html");
//            
//            return ("<html><head><title>Master</title></head>\n" +
//            		"<body>Hi, I am the master!</body></html>");
//		});
//		
//  }
  public static void WorkerStatusPage(){
      get("/workerstatus",(request,response)->{
          String ip=request.ip();
          //System.out.println("ip:"+ip);
          if (request.params()==null){
              response.status(400);
              return "bad request";
          }
          int port = Integer.parseInt(request.queryParams("port"));
          String status=request.queryParams("status");
          String job=request.queryParams("job");
          String keysRead = request.queryParams("keysRead");
		  String keysWritten = request.queryParams("keysWritten");
		  String results=request.queryParams("results");
		  String ip_port=ip+":"+port;
		  HashMap<String,String> state=new HashMap <String,String>();
		  
          state.put("status",status);
          
          state.put("job",job);
          state.put("keysRead",keysRead);
          state.put("keysWritten",keysWritten);
          state.put("results",results);
          //System.out.println("ip:port"+ip_port);
          //System.out.println("KeysWriteen"+keysWritten);
         // System.out.println("results "+results);
          workers.put(ip_port,state);
          
          response.status(200);
          
          return null;
          
      });
  }
  public static void registerStatusPage(){
      get("/status",(request,response)->{
          response.type("text/html");
           StringBuilder builder = new StringBuilder();
           builder.append("<html><head><title>Master</title></head><body><table>");
           for (String key:workers.keySet()){
               builder.append("<tr><td>IP:port "+key+"</td>");
               builder.append("<td> status "+workers.get(key).get("status")+"</td>");
               
               builder.append("<td> job "+workers.get(key).get("job")+"</td>"); 
               builder.append("<td> keysRead "+workers.get(key).get("keysRead")+"</td>"); 
               builder.append("<td> keysWritten "+workers.get(key).get("keysWritten")+"</td>"); 
               builder.append("<td> results "+workers.get(key).get("results")+"</td></tr>"); 
               
           }
           builder.append("</table>");
           builder.append("<form method=\"POST\" action=\"/submit\">");
           builder.append("job : <input type=\"text\" name=\"job\"/><br/>");
           builder.append("inputdirectory : <input type=\"text\" name=\"inputdirectory\"/><br/>");
           builder.append("outputdirectory : <input type=\"text\" name=\"outputdirectory\"/><br/>");
           builder.append("mapExecutors : <input type=\"text\" name=\"mapExecutors\"/><br/>");
           builder.append("reduceExecutors : <input type=\"text\" name=\"reduceExecutors\"/><br/>");
           builder.append("<input type=\"submit\" value=\"submmiting jobs\"></form>");
           builder.append("</body></html>");
           return builder.toString();
      });
  }
  public static void shutdown(){
      get("/shutdown",(request,response)->{
          StringBuilder builder=new StringBuilder();
          builder.append("<html><head>The Master server shutdown</head></html>");
          
          Config config = new Config();
          List<String> workerList = new ArrayList<String>();
          for(String key:workers.keySet()){
                
                    workerList.add(key);
                
            }
            System.out.println(workerList.toString());
        config.put("workerList", workerList.toString());
        String[] workernames = WorkerHelper.getWorkers(config);
        System.out.println(workernames.toString());
          int i = 0;
  		for (String workeradd:workernames){
  			config.put("workerIndex", String.valueOf(i++));
			try{
				sendJob(workeradd, "GET", config, "shutdown", "");
			}catch(Exception e){
				
			};
  		}
  		return  builder.toString();
   
          
      });
  }
  
   public static void Submit(){
      
       String FILE_SPOUT = "FILE_SPOUT";
        String MAP_BOLT = "MAP_BOLT";
         String REDUCE_BOLT = "REDUCE_BOLT";
         String PRINT_BOLT = "PRINT_BOLT";
       
        post("/submit",(request,response)->{
            String job=request.queryParams("job");
            String inputdir;
            String outputdir;
            System.out.println("inputdir "+request.queryParams("inputdirectory"));
            if (request.queryParams("inputdirectory")==null){
                
                 inputdir=null;
                
            }
            else{ inputdir=request.queryParams("inputdirectory");
            }
            if (request.queryParams("outputdirectory")==null){
                
                 outputdir=null;
                
            }
            else{ outputdir=request.queryParams("outputdirectory");
            }
            String mapExecutors=request.queryParams("mapExecutors");
            String reduceExecutors=request.queryParams("reduceExecutors");
            ArrayList <String> workerList=new ArrayList<String>();
            
            if (workers.isEmpty()){
                return "no worker";
            }
            for(String key:workers.keySet()){
                if (workers.get(key).get("status").equals("idle")){
                    workerList.add(key);
                    
                }
            }
           
            
            if (workerList.isEmpty()){return "no idle worker";}
            
            Config config = new Config();
            config.put("job", job);
            config.put("workerList", workerList.toString());
            
            config.put("master", "127.0.0.1:8080");
            
            config.put("mapClass", job);
            
            config.put("reduceClass", job);
            config.put("spoutExecutors", "1");
            config.put("mapExecutors", String.valueOf(mapExecutors));
            config.put("reduceExecutors", String.valueOf(reduceExecutors));
            config.put("inputdirectory", inputdir); 
            config.put("outputdirectory", outputdir); 
            config.put("storagedirectory","./555-hw3/store");
            
           // System.out.println("workerlist "+workerList.toString());
            String[] workernames = WorkerHelper.getWorkers(config);
             System.out.println(workernames.toString());
            FileSpout spout = new WordFileSpout();
            MapBolt mapbolt = new MapBolt();
            ReduceBolt reducebolt = new ReduceBolt();
            PrintBolt printbolt = new PrintBolt();

            TopologyBuilder builder = new TopologyBuilder();
            
            builder.setSpout(FILE_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));
            builder.setBolt(MAP_BOLT, mapbolt, Integer.valueOf(config.get("mapExecutors")))
					.fieldsGrouping(FILE_SPOUT, new Fields("value"));
             builder.setBolt(REDUCE_BOLT, reducebolt, Integer.valueOf(config.get("reduceExecutors")))
					.fieldsGrouping(MAP_BOLT, new Fields("key"));
			builder.setBolt(PRINT_BOLT, printbolt, 1)
					.firstGrouping(REDUCE_BOLT);
					
            Topology topo = builder.createTopology();
            WorkerJob workerjob = new WorkerJob(topo, config);
			
		  ObjectMapper mapper = new ObjectMapper();
	      mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	      try{
	          int i=0;
	          for (String workeradd:workernames){
	              config.put("workerIndex", String.valueOf(i));
	              i=i+1;
	              //logger.debug("workeradd "+workeradd);
	              System.out.println("workeradd "+workeradd);
	              if (sendJob(workeradd, "POST", config, "definejob",
                            mapper.writerWithDefaultPrettyPrinter().writeValueAsString(workerjob)).getResponseCode() !=
                            HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job definition request failed");
                    }
	          }
	           i = 0;
                for (String workeradd : workernames) {
                    config.put("workerIndex", String.valueOf(i));
                    i=i+1;
                    if (sendJob(workeradd, "POST", config, "runjob", "").getResponseCode() !=
                            HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job execution request failed");
                    }
                }
	          
	          
	          
	      }
	      catch(Exception e){
	          //log.error("submit job failed");
	          e.printStackTrace();
	      }
            
            
            
        response.redirect("/status");
            
          return null;  
            
        });

       
   }
public  static HttpURLConnection sendJob(String ip_port, String requestType, Config config, String jobaction, String parameters) throws IOException {
    URL url = new URL(ip_port + "/" + jobaction);
    HttpURLConnection connect = (HttpURLConnection)url.openConnection();
		connect.setDoOutput(true);
		connect.setRequestMethod(requestType);
		
		if (requestType.equals("POST")) {
			connect.setRequestProperty("Content-Type", "application/json");
			
			OutputStream os = connect.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else{
			System.out.println(connect.getResponseCode());
			System.out.println(connect.getResponseMessage());
			connect.getOutputStream();
		}
		
		return connect;
    
}


  

  /**
   * The mainline for launching a MapReduce Master.  This should
   * handle at least the status and workerstatus routes, and optionally
   * initialize a worker as well.
   * 
   * @param args
   */
    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);
		port(myPort);
		
		System.out.println("Master node startup");
		
		// TODO: you may want to adapt parts of edu.upenn.cis.stormlite.mapreduce.TestMapReduce
		// here
		
		registerStatusPage();
		WorkerStatusPage();
		Submit();
		shutdown();
		// TODO: route handler for /workerstatus reports from the workers
	}
}
  
