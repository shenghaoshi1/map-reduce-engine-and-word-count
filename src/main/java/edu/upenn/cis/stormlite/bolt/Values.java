package edu.upenn.cis.stormlite.bolt;

import java.util.*;
import java.io.Serializable;

public class Values implements Serializable{
    
    private String key;
    private ArrayList<String> values;
    public Values(String key){
        this.key=key;
        values=new ArrayList<String>();
    }
    
    public void addvalue(String value){
        values.add(value);
    }
    public String getkey(){
        return this.key;
    }
    public ArrayList<String> getvalues(){
        return this.values;
    }
    
}