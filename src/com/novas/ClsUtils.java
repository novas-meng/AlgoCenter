package com.novas;

import java.lang.reflect.Field;
import java.util.HashMap;

public class ClsUtils {
	 public static final String packageName="com.novas.";
	 //params {m=3}{n=2}
     public static Object generate(String algoname,String params) throws ClassNotFoundException, InstantiationException, IllegalAccessException
     {
    	 Class cls=Class.forName(packageName+algoname+"Params");
    	 Object obj=cls.newInstance();
    	 HashMap<String,String> valueMap=toParamsValueHashMap(params);
    	 System.out.println("valueMap="+valueMap);
    	 Field[] fields=cls.getDeclaredFields();
    	 for(int i=0;i<fields.length;i++)
    	 {
    		 fields[i].setAccessible(true);
    		 String value=valueMap.get(fields[i].getName());
    		 System.out.println("name="+fields[i].getName()+"   "+value);
    		 Class c=fields[i].getType();
    		 System.out.println("c="+c.getName()+"  "+(c==Integer.TYPE));
    		 if(c==String.class)
    		 {
    			 fields[i].set(obj, value);
    		 }
    		 else if(c==Integer.TYPE)
    		 {
    			 System.out.println("设置int");
    			 fields[i].setInt(obj, Integer.valueOf(value));
    		 }
    		 else if(c==Double.TYPE)
    		 {
    			 fields[i].set(obj, Double.valueOf(value));
    		 }
    	 } 	 
    	 return obj;
     }
     public static HashMap<String,String> toParamsValueHashMap(String params)
     {
    	 int index;
    	 int beginindex=0;
    	 int endindex=-1;
    	 char beginch='{';
    	 char endch='}';
    	 HashMap<String,String> map=new HashMap<>();
    	 while((beginindex=params.indexOf(beginch,endindex))!=-1)
    	 {
    		 endindex=params.indexOf(endch,beginindex);
    		 String var1=params.substring(beginindex+1,endindex);
    		 String[] var2=var1.split("=");
    		 map.put(var2[0], var2[1]);
    	 }
    	 return map;
     }
}
