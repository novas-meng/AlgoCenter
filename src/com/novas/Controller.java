package com.novas;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class Controller {
	private static ParamsManager manager;
     public static void main(String[] args)
     {
    	 Constants.init();
    	 System.out.println(Constants.HADOOP_PATH);
    	 manager=ParamsManager.getParamsManagerInstance();
    	 if(args.length!=0)
    	 {
    		 long time=Long.parseLong(args[0]);
    		 String name=args[1];
    		 String params=args[2];
    		 try {
				Object obj=ClsUtils.generate(name, params);
				manager.addParamsValue(time, obj);
				Class<?> cls=Class.forName("com.novas."+name);
				Algo algo=(Algo) cls.newInstance();
				try {
					algo.run(time);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (ClassNotFoundException | InstantiationException
					| IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		 
    	 }
     }
}
