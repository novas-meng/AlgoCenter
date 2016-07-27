package com.novas;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.novas.QR.EigenMapper;
import com.novas.QR.EigenReducer;
import com.novas.QR.LUMapper;
import com.novas.QR.LUReducer;
import com.novas.QR.MartixMulMapper;
import com.novas.QR.MartixMulReducer;
import com.novas.QR.NewHessenbergMapper;
import com.novas.QR.NewHessenbergReducer;
import com.novas.QR.QRMapper;
import com.novas.QR.QRReducer;
import com.novas.QR.TransMapper;
import com.novas.QR.TransReducer;
import com.novas.QR.UMapper;
import com.novas.QR.UReducer;

public class pca {
	//求矩阵转置的mapper
		public static class TransMapper extends Mapper<LongWritable,Text,IntWritable,Text>
		{
	         IntWritable transmapkey=new IntWritable();
	         Text transmapvalue=new Text();
	         int count=0;
			@Override
			protected void map(LongWritable key, Text value,
					org.apache.hadoop.mapreduce.Mapper.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				String[] var=value.toString().split(",");
				for(int i=0;i<var.length;i++)
				{
					transmapkey.set(i);
					transmapvalue.set(count+"_"+var[i]);
					//System.out.println(transmapvalue.get());
					context.write(transmapkey, transmapvalue);
				}
				count++;
			}
		}
		public static class TransReducer extends Reducer<IntWritable,Text,IntWritable,Text>
		{
	        IntWritable reducekey=new IntWritable();
	        Text reducevalue=new Text();
	        String[] strs=new String[10000];
			@Override
			protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
		        int count=0;
				for(Text value:arg1)
				{
					//reducevalue.set(value.toString());
					String[] var=value.toString().split("_");
					strs[Integer.parseInt(var[0])]=var[1];
					System.out.println("==="+strs[Integer.parseInt(var[0])]);
					count++;
				}
				StringBuilder sb=new StringBuilder();
				for(int i=0;i<count-1;i++)
				{
					sb.append(strs[i]).append(",");
				}
				sb.append(strs[count-1]);
				reducevalue.set(sb.toString());
				arg2.write(null, reducevalue);
			}
		}
		//计算协方差矩阵
	public static class COVMapper extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		//index表示第几行
        int index=0;
        int allcount=3;
        IntWritable mapkey=new IntWritable();
        Text mapvalue=new Text();
      //将Text转化为数组
        public  double[] getArray(String var1)
        {
     	    String[] var3=var1.split(",");
     	    double[] var4=new double[var3.length];
     	    for(int i=0;i<var4.length;i++)
     	    {
     	    	var4[i]=Double.valueOf(var3[i]);
     	    } 
     	    return var4;
        }
        //把数组转换为String
        public  String getString(double[] d)
        {
     	   StringBuilder sb=new StringBuilder();
     	   int i=0;
     	   for( i=0;i<d.length-1;i++)
     	   {
     		   sb.append(d[i]+",");
     	   }
     	   sb.append(d[i]);
     	   return sb.toString();
        }
        public double[] getAvgVector(String text)
        {
        	double[] vector=getArray(text);
        	double sum=0;
        	for(int i=0;i<vector.length;i++)
        	{
        		sum=sum+vector[i];
        	}
        	double avg=sum/vector.length;
        	for(int i=0;i<vector.length;i++)
        	{
        		vector[i]=vector[i]-avg;
        	}
        	return vector;
        }
        //计算协方差
        public double getCov(double[] X,double[] Y,int count)
        {
        	double sum=0;
        	for(int i=0;i<X.length;i++)
        	{
        		sum=sum+X[i]*Y[i];
        	}
        	return sum/(count-1);
        }
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			Path p=new Path(conf.get("HDFS"));
			FileSystem fs = p.getFileSystem ( conf) ;
			Path var1=new Path(p.toString()+"/home/novas/PCA/transout/part-r-00000");
				//mapkey.set(value.toString());
				//mapvalue.set(index+"_"+value.toString());
				double[] X=getAvgVector(value.toString());
				FSDataInputStream fsdis=fs.open(var1);
				String line=fsdis.readLine();
				int colum=0;
				while(line!=null)
				{
					double[] Y=getAvgVector(line);
					mapkey.set(index);
					mapvalue.set(colum+"_"+getCov(X,Y,allcount));
					colum++;
					line=fsdis.readLine();
					context.write(mapkey, mapvalue);
				}
				fsdis.close();
			index++;
		}
	}
	public static class COVReducer extends Reducer<IntWritable,Text,Text,Text>
	{
		String[] strs=new String[10000];
		Text reducevalue=new Text();
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1,
				Context arg2) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			System.out.println("arg0="+arg0);
			for(Text val:arg1)
			{
				System.out.println("val="+val);
				String[] var=val.toString().split("_");
				strs[Integer.valueOf(var[0])]=var[1];
				count++;
			}
			StringBuilder sb=new StringBuilder();
			for(int i=0;i<count-1;i++)
			{
				sb.append(strs[i]+",");
			}
			sb.append(strs[count-1]);
			reducevalue.set(sb.toString());
			arg2.write(null, reducevalue);
		}
	}
	 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	   {
		      long atime=System.currentTimeMillis();
			  ParamsManager manager=ParamsManager.getParamsManagerInstance();
			  double  J = Double.MAX_VALUE;//J表示价值函数的值
			  double flag = 5;//自己定义的阈值
			 // int c=3;
			//  int m=2;
			  Configuration conf = new Configuration ( ) ;
			  //Configuration.addResource("/usr/hadoop/hadoop-1.2.1/conf/core-site.xml");
			  conf.addResource(new Path("/usr/hadoop/hadoop-1.2.1/conf/core-site.xml"));
		
			  Path p=new Path(conf.get("fs.default.name"));
			  conf.set("HDFS", conf.get("fs.default.name"));
			  FileSystem fs = p.getFileSystem ( conf ) ;
			  String parentpath=p.toString();
			//  FileStatus[] fstatus = fs.listStatus ( new Path (p,"/home/novas") ) ;
			  /*
			   * 配置数据存储相关路径
			   */
			  String deletePath = null ;
			  String readPath = null ;//这个参数的含义是用来交替改变读取位置的
			  readPath=parentpath+"/home/novas/PCA/input";
			  String out=parentpath+"/home/novas/PCA/out";
			  //转置矩阵输出路径
			  String transmartixout=parentpath+"/home/novas/PCA/transout";
			  //协方差输出路径
			  String covout=parentpath+"/home/novas/PCA/covout";
			  //hessenberg矩阵输出路径
			  String hessenbergout=parentpath+"/home/novas/PCA/hessenbergout";
			  String qrout=parentpath+"/home/novas/PCA/qrout";
			  String eigenout=parentpath+"/home/novas/PCA/eigenout";
			  String eigenvectorout=parentpath+"/home/novas/PCA/eigenvectorout";
			  String uout=parentpath+"/home/novas/PCA/U";
			  fs.delete(new Path(uout));
			  fs.delete(new Path(out));
			  fs.delete(new Path(transmartixout));
			  fs.delete(new Path(covout));
			  fs.delete(new Path(hessenbergout));
			  fs.delete(new Path(qrout));
			  fs.delete(new Path(eigenout));
			  fs.delete(new Path(eigenvectorout));
	//求矩阵转置的job
			  Job transjob = new Job ( conf ) ;
			  transjob.setJarByClass(QR.class);
			 //设定一些常量参数
			  transjob.setMapperClass ( TransMapper.class ) ;
			  transjob.setReducerClass ( TransReducer.class ) ;
			  transjob.setMapOutputKeyClass(IntWritable.class);
			  transjob.setMapOutputValueClass(Text.class);
			  transjob.setOutputKeyClass ( Text.class);
			  transjob.setOutputValueClass ( Text.class);
		 	
		 	 FileInputFormat.addInputPath ( transjob , new Path ( readPath ) ) ;
		 	 FileOutputFormat.setOutputPath ( transjob ,  new  Path ( transmartixout ) ) ;
		 	 transjob.waitForCompletion ( true ) ;
		 	 
		 	 //求协方差矩阵
		 	  Job covjob = new Job ( conf ) ;
		 	 covjob.setJarByClass(QR.class);
			 //设定一些常量参数
		 	covjob.setMapperClass (COVMapper .class ) ;
		 	covjob.setReducerClass ( COVReducer.class ) ;
		 	covjob.setMapOutputKeyClass(IntWritable.class);
		 	covjob.setMapOutputValueClass(Text.class);
		 	covjob.setOutputKeyClass ( IntWritable.class);
		 	covjob.setOutputValueClass ( Text.class);
		 	
		 	 FileInputFormat.addInputPath ( covjob , new Path ( transmartixout ) ) ;
		 	 FileOutputFormat.setOutputPath ( covjob ,  new  Path ( covout ) ) ;
		 	covjob.waitForCompletion ( true ) ;
		 	 /*
		 	 //计算上面job得到结果的hessenberg矩阵
		 //	  Job hessenbergjob = new Job ( conf ) ;
		 	// hessenbergjob.setJarByClass(QR.class);
				 //设定一些常量参数
		 //	hessenbergjob.setMapperClass (HessenbergMapper .class ) ;
		 //	hessenbergjob.setReducerClass ( HessenbergReducer.class ) ;
		 //	hessenbergjob.setMapOutputKeyClass(IntWritable.class);
		// 	hessenbergjob.setMapOutputValueClass(Text.class);
		// 	hessenbergjob.setOutputKeyClass ( Text.class);
		// 	hessenbergjob.setOutputValueClass ( Text.class);
		//	FileInputFormat.addInputPath ( hessenbergjob , new Path ( martixmulout ) ) ;
		//    FileOutputFormat.setOutputPath ( hessenbergjob ,  new  Path ( hessenbergout ) ) ;
			//hessenbergjob.waitForCompletion ( true ) ;
		 	 int count=0;
		 	 while(true)
		 	 {
		 		  //qr分解
				  Job qrjob = new Job ( conf ) ;
				  qrjob.setJarByClass(QR.class);
				 //设定一些常量参数
				  qrjob.setMapperClass ( QRMapper.class ) ;
				  qrjob.setReducerClass ( QRReducer.class ) ;
				  qrjob.setMapOutputKeyClass(IntWritable.class);
				  qrjob.setMapOutputValueClass(Text.class);
				  qrjob.setOutputKeyClass ( IntWritable.class);
				  qrjob.setOutputValueClass ( Text.class);
			 	
			 	 FileInputFormat.addInputPath ( qrjob , new Path ( hessenbergout ) ) ;
			 	 FileOutputFormat.setOutputPath ( qrjob ,  new  Path ( qrout ) ) ;
			      qrjob.waitForCompletion ( true ) ;
			      
			      fs.delete(new Path(hessenbergout));
				  //生成新的hessenberg矩阵，进行下次qr迭代
				  Job newhessenbergjob = new Job ( conf ) ;
				  newhessenbergjob.setJarByClass(QR.class);
				 //设定一些常量参数
				  newhessenbergjob.setMapperClass ( NewHessenbergMapper.class ) ;
				  newhessenbergjob.setReducerClass ( NewHessenbergReducer.class ) ;
				  newhessenbergjob.setMapOutputKeyClass(IntWritable.class);
				  newhessenbergjob.setMapOutputValueClass(Text.class);
				  newhessenbergjob.setOutputKeyClass ( Text.class);
				  newhessenbergjob.setOutputValueClass ( Text.class);
			 	
			 	 FileInputFormat.addInputPath ( newhessenbergjob , new Path ( qrout ) ) ;
			 	 FileOutputFormat.setOutputPath ( newhessenbergjob ,  new  Path ( hessenbergout ) ) ;
			 	 newhessenbergjob.waitForCompletion ( true ) ;
			     count++;
			     if(count==3)
			     {
			    	 break;
			     }
			     else
			     {
				     fs.delete(new Path(qrout));
			     }
		 	 }
		  
		 //矩阵特征值
			  Job eigenjob = new Job ( conf ) ;
			  eigenjob.setJarByClass(QR.class);
			 //设定一些常量参数
			     eigenjob.setMapperClass ( EigenMapper.class ) ;
			     eigenjob.setReducerClass ( EigenReducer.class ) ;
			     eigenjob.setMapOutputKeyClass(IntWritable.class);
			     eigenjob.setMapOutputValueClass(DoubleWritable.class);
			     eigenjob.setOutputKeyClass ( IntWritable.class);
			     eigenjob.setOutputValueClass ( DoubleWritable.class);
		 	 FileInputFormat.addInputPath ( eigenjob , new Path ( qrout ) ) ;
		 	 FileOutputFormat.setOutputPath ( eigenjob ,  new  Path ( eigenout ) ) ;
		     eigenjob.waitForCompletion ( true ) ;
		     //求矩阵特征向量
		     Job eigenvectorjob = new Job ( conf ) ;
		     eigenvectorjob.setJarByClass(QR.class);
		     eigenvectorjob.setMapperClass ( LUMapper.class ) ;
		     eigenvectorjob.setReducerClass ( LUReducer.class ) ;
		     eigenvectorjob.setMapOutputKeyClass(DoubleWritable.class);
		     eigenvectorjob.setMapOutputValueClass(Text.class);
		     eigenvectorjob.setOutputKeyClass ( DoubleWritable.class);
		     eigenvectorjob.setOutputValueClass ( Text.class);
		 	 FileInputFormat.addInputPath ( eigenvectorjob , new Path ( martixmulout ) ) ;
		 	 FileOutputFormat.setOutputPath ( eigenvectorjob ,  new  Path ( eigenvectorout ) ) ;
		     eigenvectorjob.waitForCompletion ( true ) ;
		     //求SVD分解的U
		     Job ujob = new Job ( conf ) ;
		     ujob.setJarByClass(QR.class);
		     ujob.setMapperClass ( UMapper.class ) ;
		     ujob.setReducerClass ( UReducer.class ) ;
		     ujob.setMapOutputKeyClass(IntWritable.class);
		     ujob.setMapOutputValueClass(Text.class);
		     ujob.setOutputKeyClass ( Text.class);
		     ujob.setOutputValueClass ( Text.class);
		 	 FileInputFormat.addInputPath ( ujob , new Path ( readPath ) ) ;
		 	 FileOutputFormat.setOutputPath ( ujob ,  new  Path ( uout ) ) ;
		 	ujob.waitForCompletion ( true ) ;
		 	*/
	   }
}
