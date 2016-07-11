package com.novas;

import java.lang.reflect.Field;
import java.util.HashMap;

public class ParamsManager {
	 private HashMap<Long,Object> paramsMap=new HashMap<Long,Object>();
     private static  ParamsManager manager=null;
     private ParamsManager()
     {
    	 
     }
     //单例模式
     public static ParamsManager getParamsManagerInstance()
     {
    	 if(manager==null)
    	 {
    		 manager=new ParamsManager();
    	 }
    	return manager;
     }
     //添加参数类
     public void addParamsValue(long time,Object paramsValue)
     {
    	 paramsMap.put(time, paramsValue);
     }
     //获取参数类  
     public Object getParamsValue(long time,String name) 
     {
    	 Object val=paramsMap.get(time);
    	 Class cls=val.getClass();
    	 System.out.println("clsname="+cls.getName());
    	 try {
			Field f=cls.getDeclaredField(name);
			f.setAccessible(true);
			return f.get(val);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	 return null;
     }
}
