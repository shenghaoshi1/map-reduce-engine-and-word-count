package edu.upenn.cis.stormlite.distributed;

import java.util.Map;

public class WorkerHelper {
	
	/**
	 * Get the list of workers
	 * 
	 * @param config
	 * @return
	 */
	public static String[] getWorkers(Map<String, String> config) {
		String list = config.get("workerList");
		String target;
		if (list.startsWith("["))
			list = list.substring(1);
		if (list.endsWith("]"))
			list = list.substring(0, list.length() - 1);
		
		String[] servers = list.split(",");
		
		String[] ret = new String[servers.length];
		int i = 0;
		for (String item: servers){
		    target=item.replace(" ","");
			if (!target.startsWith("http"))
				ret[i++] = "http://" + target;
			else
				ret[i++] = target;
		}

		return ret;
	}
}
