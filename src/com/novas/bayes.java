package com.novas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class bayes implements Algo
{
	public static class ComV{  
	    String first;  
	    String second;  
	    /** 
	     * Set the left and right values. 
	     */  
	    public void set(String left, String right) {  
	        first = left;  
	        second = right;  
	    }  
	    public String getFirst() {  
	        return first;  
	    }  
	    public String getSecond() {  
	        return second;  
	    }  
	   @Override
	   public String toString()
	   {
		  return first+","+second;
		//   return super.toString();
	   }
	
	    @Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode()+second.hashCode();
	}
		//新定义类应该重写的两个方法 
	    @Override  
	    public boolean equals(Object right) {  
	        if (right instanceof ComV) {  
	        	ComV r = (ComV) right;  
	            return r.first.equals( first) && r.second.equals( second);  
	        } else {  
	            return false;  
	        }  
	    }  
	}
	public static class ComValue implements WritableComparable<ComValue> {  
	    String first;  
	    String second;  
	    /** 
	     * Set the left and right values. 
	     */  
	    public void set(String left, String right) {  
	        first = left;  
	        second = right;  
	    }  
	    public String getFirst() {  
	        return first;  
	    }  
	    public String getSecond() {  
	        return second;  
	    }  
		public int hashCode() {
			// TODO Auto-generated method stub
			return first.hashCode()+second.hashCode();
		}
	    @Override  
	    //反序列化，从流中的二进制转换成IntPair  
	    public void readFields(DataInput in) throws IOException {  
	        // TODO Auto-generated method stub  
	        first = in.readUTF();  
	        second = in.readUTF();  
	    }  
	    @Override  
	    //序列化，将IntPair转化成使用流传送的二进制  
	    public void write(DataOutput out) throws IOException {  
	        // TODO Auto-generated method stub  
	        out.writeUTF(first);  
	        out.writeUTF(second);  
	    }  
	    @Override  
	    //key的比较  
	    public int compareTo(ComValue o) {  
	        // TODO Auto-generated method stub  
	 //   	System.out.println(this.toString()+"    "+o.toString());
	        if (!first.equals( o.first)) {  
	            return first .compareTo(o.first)<0 ? -1 : 1;  
	        } else if (!second .equals(o.second)) {  
	            return second.compareTo( o.second)<0 ? -1 : 1;  
	        } else {  
	            return 0;  
	        }  
	    }  
	   @Override
	   public String toString()
	   {
		  return first+","+second;
		//   return super.toString();
	   }
	    //新定义类应该重写的两个方法  
	    @Override  
	    public boolean equals(Object right) {  
	     
	        if (right instanceof ComValue) {  
	        	ComValue r = (ComValue) right;  
	        //	System.out.println("r="+r);
	            boolean b= r.first.equals( first) && r.second.equals( second);  
	            System.out.println(b);
	            return b;
	        } else {  
	            return false;  
	        }  
	    }  
	}
	public static class DataMapper extends Mapper<LongWritable,Text,Text,ComValue>
	{
        Text mapkey=new Text();
        ComValue mapvalue=new ComValue();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		//	super.map(key, value, context);
			String[] var1=value.toString().split(",");
			mapkey.set(var1[var1.length-1]);
			for(int i=0;i<var1.length-1;i++)
			{
				mapvalue.set(i+1+"",var1[i]);
			//	System.out.println(mapvalue);
				context.write(mapkey, mapvalue);
			}
		}
	}

	public static class DataReducer extends Reducer<Text,ComValue,Text,DoubleWritable>
	{
       Text reduceKey=new Text();
       DoubleWritable reduceValue=new DoubleWritable();
       HashMap<String,Double> categorycountmap=new HashMap();
       //表示的含义是 1，set 表示第一个特征的所有取值情况
       HashMap<String,Set<String>> charactermap=new HashMap();
		@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
			double N=0;
			double K=categorycountmap.size();
			Configuration conf=context.getConfiguration() ;
			Path p=new Path(conf.get("HDFS"));
			FileSystem fs = p.getFileSystem ( conf) ;
			FSDataOutputStream fsos=fs.create(new Path(conf.get("trainout")+"/category"));
			fsos.writeInt(categorycountmap.size());
			for(Map.Entry<String, Double> entry:categorycountmap.entrySet())
			{
				N=N+entry.getValue();
			}
			for(Map.Entry<String, Double> entry:categorycountmap.entrySet())
			{
				N=N+entry.getValue();
				String category=entry.getKey();
				double count=entry.getValue();
				System.out.println("category="+category+"count="+count);
				fsos.writeUTF(category);
				fsos.writeDouble((count+1)/(N+K));
			}
			fsos.close();
	}

		@Override
		protected void reduce(Text arg0, Iterable<ComValue> arg1,Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		//	super.reduce(arg0, arg1, arg2);
			//这个map存放的是同一个类别下的特征概率，例子，key=1,2 value=0.4 表示当第一个特征值取值为2的时候，概率为0.4
			HashMap<ComV,Double> map=new HashMap();
			System.out.println(arg0);
			double count=0;
			for(ComValue var:arg1)
			{
		//		System.out.println(var);
				ComV cv=new ComV();
				cv.set(var.getFirst(), var.getSecond());
				Object obj=map.get(cv);
				if(cv.getFirst().equals("1"))
				{
					count++;
				}
				//添加每个特征的所有可能取值
				if(charactermap.get(cv.getFirst())==null)
				{
					Set<String> set=new HashSet<String>();
					set.add(cv.getSecond());
					charactermap.put(cv.getFirst(),set);
				}
				else
				{
					Set set=charactermap.get(cv.getFirst());
					set.add(cv.getSecond());
					charactermap.put(cv.getFirst(), set);
				}
				if(obj==null)
				{
					map.put(cv, 1.0);
				}
			else
		    	{
				   Double in=(Double)obj;
					map.put(cv, in.doubleValue()+1);
				}

			}
			System.out.println(map);
            for(Map.Entry<ComV, Double> entry:map.entrySet())
            {
            	//获取Sj的值
            	String var1=entry.getKey().getFirst();
            	int sj=charactermap.get(var1).size();
            	reduceKey.set(entry.getKey().toString()+","+arg0);
            	reduceValue.set((entry.getValue()+1)/(count+sj));
            	arg2.write(reduceKey, reduceValue);
            }
            categorycountmap.put(arg0.toString(), count);
		}
	}
	
	//下面的是进行预测的job
	public static class PredictMapper  extends Mapper<LongWritable,Text,Text,Text>
	{
       HashMap<String,Double> charactermap=new HashMap<String,Double>();
       HashMap<String,Double> categorymap=new HashMap();
	   Text seqkey=new Text();
	   DoubleWritable seqvalue=new DoubleWritable();
	   Text mapkey=new Text();
	   Text mapvalue=new Text();
   	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
   		Configuration conf=context.getConfiguration() ;
		Path p=new Path(conf.get("HDFS"));
		 FileSystem fs = p.getFileSystem ( conf) ;
	//	SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path("hdfs://localhost:9000/home/novas/clusterpointoutput/part-r-00000"), context.getConfiguration());
		 SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path(conf.get("trainout")+"/part-r-00000"),conf);
		  while(reader.next(seqkey, seqvalue))
	   {
			  charactermap.put(seqkey.toString(), seqvalue.get());
	   }
		  System.out.println(charactermap);
		  FSDataInputStream fsdis=fs.open(new Path(conf.get("trainout")+"/category"));
		  int count=fsdis.readInt();
		  System.out.println("count="+count);
		  for(int i=0;i<count;i++)
		  {
			  categorymap.put(fsdis.readUTF(),fsdis.readDouble());
			///  System.out.println(categorymap);
		  }
		  System.out.println(categorymap);
	}
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] var=value.toString().split(",");
			String category=null;
			double max=-1;
			for(String str:categorymap.keySet())
			{
				double prob=1;
				double categoryD=categorymap.get(str);
				prob=prob*categoryD;
				for(int i=0;i<var.length;i++)
				{
					String var1=i+1+","+var[i]+","+str;
				//	System.out.println("var1="+var1);
					if(charactermap.containsKey(var1))
					{
						prob=prob*charactermap.get(var1);
                        System.out.println("var="+var1+"    "+charactermap.get(var1)+"   "+prob);
					}
					else
					{
						prob=prob*0;
					}
				}
				if(prob>max)
				{
					max=prob;
					category=str;
				}
			}
			mapkey.set(value.toString());
			mapvalue.set(category);
			context.write(mapkey, mapvalue);
		}
	}
	public static class PredictReducer extends Reducer<Text,Text,Text,Text>
	{

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text var:arg1)
			{
				arg2.write(arg0, var);
			}
		}
	}
    public void run(long timestamp) throws IOException, ClassNotFoundException, InterruptedException
    {
    	//读取路径
    	String trainInputPath=null;
  	    ParamsManager manager=ParamsManager.getParamsManagerInstance();
    	Configuration conf=new Configuration();
  	    conf.addResource(new Path(Constants.HADOOP_PATH+"/conf/core-site.xml"));
    	String hdfs=conf.get("fs.default.name");
    	System.out.println(hdfs);
    	bayesParams params=new bayesParams();
    	Path p=new Path(hdfs);
    	conf.setStrings("HDFS", hdfs);
    	FileSystem fs=p.getFileSystem(conf);
    	String parentpath=p.toString();
    	
    	trainInputPath=parentpath+manager.getParamsValue(timestamp, "traininputPath");
    	
    	String trainOutputPath=parentpath+"/"+timestamp+"/bayesout";
    	conf.set("trainout", trainOutputPath);
    	String predictReadPath=parentpath+manager.getParamsValue(timestamp,"predictInputPath");
    	String predictOutPath=parentpath+"/"+timestamp+manager.getParamsValue(timestamp,"outputPath");
    	fs.delete(new Path(trainOutputPath));
        fs.delete(new Path(predictOutPath));
    	Job job1=new Job(conf);
    	job1.setJarByClass(bayes.class);
    	job1.setMapperClass(DataMapper.class);
    	job1.setReducerClass(DataReducer.class);
    	job1.setMapOutputKeyClass(Text.class);
    	job1.setMapOutputValueClass(ComValue.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(DoubleWritable.class);
    	 job1.setOutputFormatClass ( SequenceFileOutputFormat.class ) ;
    	FileInputFormat.addInputPath ( job1 , new Path ( trainInputPath ) ) ;
    	SequenceFileOutputFormat.setOutputPath(job1, new Path(trainOutputPath));
    	job1.waitForCompletion(true);
    	
    	
    	Job predictJob=new Job(conf);
    	predictJob.setJarByClass(bayes.class);
    	predictJob.setMapperClass(PredictMapper.class);
    	predictJob.setReducerClass(PredictReducer.class);
    	predictJob.setOutputKeyClass(Text.class);
    	predictJob.setOutputValueClass(Text.class);
    	//predictJob.setOutputFormatClass ( FileOutputFormat.class ) ;
    	FileInputFormat.addInputPath ( predictJob , new Path ( predictReadPath ) ) ;
    	FileOutputFormat.setOutputPath(predictJob, new Path(predictOutPath));
    	predictJob.waitForCompletion(true);   	
    }
}
