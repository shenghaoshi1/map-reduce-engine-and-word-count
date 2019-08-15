package edu.upenn.cis.stormlite.bolt;

import java.util.*;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;
import java.io.FileNotFoundException;

public class DBWrapper{
    private String dir;
    private Environment env;
    private static final String CLASS_CATALOG = "java_class_catalog";
    private StoredClassCatalog javaCatalog;
    
    private Map<String,Values> map;
    private static final String STORE="STORE";
    private Database Db;
    
    public DBWrapper(String dir){
        this.dir=dir;
        File directory = new File(this.dir);
        if (!directory.exists()) {
            directory.mkdir();
        }
        System.out.println("Opening environment in: " + this.dir);
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        this.env = new Environment(new File(this.dir), envConfig);
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, 
                                              dbConfig);

        javaCatalog = new StoredClassCatalog(catalogDb);
        this.Db =env.openDatabase(null, STORE, dbConfig);
        
        EntryBinding<String> stringBinding = new StringBinding();
        EntryBinding<Values> ValuesBinding = new SerialBinding<Values>(javaCatalog, Values.class);
        map= new StoredSortedMap<String,Values>(Db, stringBinding, ValuesBinding , true);
        
        
    }
     public final Database getDb(){
             return this.Db;
         }
         
    public void close()throws DatabaseException{
        this.Db.close();
        this.javaCatalog.close();
        this.env.close();
    }
    public synchronized void addkeyvalue(String key, String value){
        if(!map.containsKey(key)){
            Values values=new Values(key);
            values.addvalue(value);
            map.put(key,values);
            
        }
        else{
            Values values=map.get(key);
             values.addvalue(value);
            map.put(key,values);
            
        }
        
    }
    public Values getvalues(String key){
        if (!map.containsKey(key)){
            return null;
        }
        return map.get(key);
    }
    
    public ArrayList<String > getkeys(){
        ArrayList <String > keys=new ArrayList<String >();
        for(String key:map.keySet()){
            keys.add(key);
            
        }
        return keys;
    }
}
