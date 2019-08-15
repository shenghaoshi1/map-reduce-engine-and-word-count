package edu.upenn.cis.stormlite.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import edu.upenn.cis.stormlite.Config;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import java.io.File;
public class WordFileSpout extends FileSpout{
    @Override
    
    public String getFilename(){
        String dir;
        String inputdir;
        String filename="";
       // System.out.println("inputdirectory: "+getconfig().get("inputdirectory"));
        if (getconfig().get("inputdirectory")==null||getconfig().get("inputdirectory").equals("")){
             inputdir="";
             dir=getconfig().get("storagedirectory");
        }
        else{
             inputdir=getconfig().get("inputdirectory");
             dir=getconfig().get("storagedirectory")+"/"+inputdir;
        }
        //String dir=getconfig().get("storagedirectory")+"/"+inputdir;
        log.debug("dir info: "+dir);
       // System.out.println("dir info: "+dir);
        File folder = new File(dir);
        System.out.println(folder.exists());
        System.out.println(folder.getAbsolutePath());
        String[] files = folder.list();
        System.out.println(files);
        for (String file:files){
            
            String[] parts = file.split("\\.");
            if (parts.length>1){
                filename=parts[0]+"."+parts[1];
                break;
            }
            else{
                filename=parts[0];
            }
            
        }
        return dir+"/"+filename;
    }
    
}