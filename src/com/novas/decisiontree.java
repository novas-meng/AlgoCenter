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

import com.novas.bayes.ComV;
import com.novas.bayes.ComValue;
public class decisiontree implements Algo {
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
		  return first+":"+second;
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
	
	}
	public static class trainMapper extends Mapper<LongWritable,Text,Text,ComValue>
	{
		public String preffix="";
		public Text mapkey=new Text();
		//过滤条件 形式为1->青年，2->一般这样的
		public HashMap<String,String> flitermap=new HashMap<String,String>();
		//剩下的可以使用的特征
		public ArrayList<String> availChartList=new ArrayList<String>();
		FileSystem fs=null;
		public String time=null;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			time=conf.get("time");
			String currentnodeString=conf.get("currentnode");
			if(currentnodeString!=null)
			{
				System.out.println("currentnode="+currentnodeString);
				String[] args=currentnodeString.split(",");
				for(int i=0;i<args.length;i++)
				{
					String[] var=args[i].split(":");
					flitermap.put(var[0], var[1]);
				}
			}
		
			Path p=new Path(conf.get("HDFS"));
			fs = p.getFileSystem ( conf) ;
			Path var1=new Path(conf.get("tempout")+"/characterarray");
			if(fs.exists(var1))
			{
				FSDataInputStream fsdis=fs.open(var1);
				int var2=fsdis.readInt();
				for(int i=0;i<var2;i++)
				{
					availChartList.add(fsdis.readUTF());
				}
				fsdis.close();
			}
			
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			String[] var=value.toString().split(",");
		  //  Configuration conf=context.getConfiguration() ;
		//	Path p=new Path(conf.get("HDFS"));
		//	 FileSystem fs = p.getFileSystem ( conf) ;
			//获取所有特征
			Path var1=new Path(conf.get("tempout")+"/characterarray");
			if(!fs.exists(var1))
			{
				System.out.println("获取所有属性");
				FSDataOutputStream fsdos=fs.create(var1);
				fsdos.writeInt(var.length-1);
				for(int i=0;i<var.length-1;i++)
				{
					availChartList.add(i+1+"");
					fsdos.writeUTF(i+1+"");
				}
				fsdos.close();
			}
			boolean forReduce=true;
			//检测样本是否满足条件，包含currentnode这个路径 形式为1:青年,1:中年
			for(Map.Entry<String, String> entry:flitermap.entrySet())
			{
				int index=Integer.parseInt(entry.getKey());
				String var4=entry.getValue();
				//举例 对于{青年 是 一般 }这样的样本，如果currentnode中为1:中年，那么这个样本是不满足条件的。不用在reduce中处理
				if(!var[index-1].equals(var4))
				{
					forReduce=false;
					break;
				}
			}
			if(forReduce)
			{
				System.out.println(value.toString());
				if(availChartList.size()==0)
				{
					ComValue cv=new ComValue();
					cv.set("1", time);
					//var[var.length-1]表示的是类别
					mapkey.set(var[var.length-1]);
					context.write(mapkey, cv);
				}
				else
				{
					for(int i=0;i<var.length-1;i++)
					{
						//剩余特征中包含该特征，该样本进入reduce阶段
						if(availChartList.contains(i+1+""))
						{
							ComValue cv=new ComValue();
							cv.set(i+1+"", var[i]);
							mapkey.set(var[var.length-1]);
							context.write(mapkey, cv);
						}
					}
				}	
			}
		}
	}
	public static class trainReducer extends Reducer<Text,ComValue,Text,Text>
	{
		//存放的是分类类别和对应的数目
       HashMap<String,Integer> categorymap=new HashMap();
       //存放的是gda计算所需数据，形式为{1：青年}-》{C1:2,C2:3} 其中C1,C2 为类别，后面为出现的次数
       HashMap<ComV,ArrayList<ComV>> gdamap=new HashMap();
       //存放的是每个特征的AG值
       HashMap<String,Double> agmap=new HashMap<String,Double>();
       //存放的是特征和特征对应的取值,比如1->青年，中年，老年
       HashMap<String,ArrayList<String>> charactermap=new HashMap();
       //存放特征和对应的HAD值
       HashMap<String,Double> HADmap=new HashMap();
       //time用来存放当特征没有的时候的那个标示，比如1:time
       String time=null;
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("gda"+Math.log(3.0/10));
			System.out.println(categorymap);
			System.out.println(gdamap);
			Configuration conf=context.getConfiguration() ;
			time=conf.get("time");
			Path p=new Path(conf.get("HDFS"));
			FileSystem fs = p.getFileSystem ( conf) ;			//首先进行判断，如果只有一个类别，那么就不用计算最大信息增益比了
			String currentnodepath=conf.get("currentnode","");
			Path pathv=new Path(conf.get("trainout")+"/decisiontree");
		//	SequenceFile.Writer writer= SequenceFile.createWriter(fs,conf,pathv,Text.class,Text.class);
			  if(categorymap.size()==1)
			{
				HashMap<String,String> treemap=new HashMap<String,String>();
				System.out.println("单节点");
				String[] st=new String[1];
				categorymap.keySet().toArray(st);
				treemap.put(currentnodepath, st[0]);
				//writer.append(reduceKey, reduceValue);
			//	writer.close();
				//context.(reduceKey, reduceValue);
				if(fs.exists(pathv))
				{
                    FSDataInputStream fdis=fs.open(pathv);
                    int c=fdis.readInt();
                    for(int i=0;i<c;i++)
                    {
                    	treemap.put(fdis.readUTF(),fdis.readUTF());
                    }
                    fdis.close();
                    treemap.put(currentnodepath, st[0]);
				}
				FSDataOutputStream fdos=fs.create(pathv);
				fdos.writeInt(treemap.size());
				for(Map.Entry<String, String> entry:treemap.entrySet())
				{
					fdos.writeUTF(entry.getKey());
					fdos.writeUTF(entry.getValue());
				}
				fdos.close();
				return;
			}
			  
			double HD=0;
			double allcount=0;
		    double HAD=0;
			//计算样本总的个数
			for(Map.Entry<String, Integer> entry:categorymap.entrySet())
			{
				allcount=allcount+entry.getValue();
			}
			for(Map.Entry<String, Integer> entry:categorymap.entrySet())
			{
				HD=HD-(entry.getValue()/allcount)*(Math.log(entry.getValue()/allcount)/Math.log(2));
			}
			//计算H（D）
			//计算H(D|A)  {2,否=[否,6, 是,4], 1,中年=[否,2, 是,3], 3,否=[否,6, 是,3], 1,青年=[否,3, 是,2], 4,非常好=[是,4], 1,老年=[否,1, 是,4], 4,一般=[否,4, 是,1], 4,好=[否,2, 是,4], 2,是=[是,5], 3,是=[是,6]}
//针对这样的形式，首先计算每一个部分的，然后将为1的加起来，就是对应第一个特征的H(D|A)
			for(Map.Entry<ComV, ArrayList<ComV>> entry:gdamap.entrySet())
			{
				ArrayList<ComV> list=entry.getValue();
				int var=0;
				//计算DI
				for(int i=0;i<list.size();i++)
				{
					var=var+Integer.parseInt(list.get(i).getSecond());
				}
				if(!HADmap.containsKey(entry.getKey().getFirst()))
				{
					HAD=HAD-var/allcount*(Math.log(var/allcount)/Math.log(2));
				}
				else
				{
					HAD=HADmap.get(entry.getKey().getFirst())-var/allcount*(Math.log(var/allcount)/Math.log(2));
				}
				HADmap.put(entry.getKey().getFirst(), HAD);
				//计算H（D|A)
				double hda=0;
				for(int i=0;i<list.size();i++)
				{
					double var1=Double.parseDouble(list.get(i).getSecond());
					hda=hda-var1/var*(Math.log(var1/var)/Math.log(2));
				}
				String key=entry.getKey().getFirst();
				//把属于同一个特征的计算结果相加
				if(!agmap.containsKey(key))
				{
					agmap.put(key,HD- hda*var/allcount);
					//添加特征可能的取值
					ArrayList<String> characterList=new ArrayList();
					characterList.add(entry.getKey().getSecond());
					charactermap.put(entry.getKey().getFirst(), characterList);
				}
				else
				{
					agmap.put(key, agmap.get(key)-hda*var/allcount);
					ArrayList<String> characterList=charactermap.get(entry.getKey().getFirst());
					characterList.add(entry.getKey().getSecond());
					charactermap.put(entry.getKey().getFirst(), characterList);
				}
			}
			System.out.println(agmap);
			System.out.println(charactermap);
			System.out.println(HADmap);
			for(Map.Entry<String, Double> entry:agmap.entrySet())
			{
				double newv=entry.getValue()/HADmap.get(entry.getKey());
				agmap.put(entry.getKey(), newv);
			}
			System.out.println(agmap);
			System.out.println(charactermap);
			System.out.println(HADmap);
			//计算出来ag之后，进行判断，首先选出ag最大的，然后与阈值进行比较
			double max=Double.MIN_VALUE;
			String character=null;
			for(Map.Entry<String, Double> entry:agmap.entrySet())
			{
				if(entry.getValue()>max)
				{
					max=entry.getValue();
					character=entry.getKey();
				}
			}
			System.out.println(max+"   "+character);
			//String[] t=new ArrayList();
		//	conf.setStrings("a", t);
			double E=conf.getFloat("E", 0);
			String path=conf.get("tempout")+"/decisionnode";
			Path nodepath=new Path(path);
			//Path p=new Path(conf.get("HDFS"));
			Path varpath=new Path(conf.get("tempout")+"/characterarray");
			System.out.println("charactmap="+charactermap.toString()+"    "+time);
			if(charactermap.toString().equals("{1=["+time+"]}"))
			{
				System.out.println("特征数已经用完");
				max=Double.MIN_VALUE;
			}
		//	FileSystem fs = p.getFileSystem ( conf) ;
			//表示在构建决策树的时候，当前的路径，也就是经过的分支的表示
			//如果AG大于阈值，那么选取AG最大的那个特征,然后将这个特征从所有特征中去除
			if(max>E)
			{
				//将特征从所有特征中去除
				FSDataInputStream fsdis=fs.open(varpath);
				int var2=fsdis.readInt();
				String[] characterarray=new String[var2];
				for(int i=0;i<var2;i++)
				{
					characterarray[i]=fsdis.readUTF();
				}
				String[] newarray=new String[characterarray.length-1];
				int d=0;
				for(int i=0;i<characterarray.length;i++)
				{
					if(!characterarray[i].equals(character))
					{
						newarray[d]=characterarray[i];
						d++;
					}
				}
				System.out.println("newArray="+newarray);
                fs.delete(varpath);		       	
				FSDataOutputStream fsdos=fs.create(varpath);
                fsdos.writeInt(newarray.length);
                for(int i=0;i<newarray.length;i++)
                {
                	fsdos.writeUTF(newarray[i]);
                }
                fsdos.close();
                //写入新的节点
			    ArrayList<String> list=new ArrayList<String>();
				FSDataInputStream fdis=fs.open(nodepath);
				int count=fdis.readInt();
				System.out.println("以前的个数  "+count);
				for(int i=0;i<count;i++)
				{
					list.add(fdis.readUTF());
				}
				fs.delete(nodepath);
				ArrayList characterlist=charactermap.get(character);
				for(int i=0;i<characterlist.size();i++)
				{
					if(currentnodepath.equals(""))
					{
						list.add(character+":"+characterlist.get(i));
					}
					else
					{
						list.add(currentnodepath+","+character+":"+characterlist.get(i));
					}
				}
				FSDataOutputStream fsdosvar=fs.create(nodepath);
				fsdosvar.writeInt(list.size());
				System.out.println("写入节点个数  "+list.size()+"   "+list);
				for(int i=0;i<list.size();i++)
				{
					fsdosvar.writeUTF(list.get(i));
				}
				fsdosvar.close();
			}
			else
			{
				//当小于阈值的时候，找到实例数最大的那个类别，将该类别作为该节点的类
				int maxcount=Integer.MIN_VALUE;
				String category=null;
				for(Map.Entry<String, Integer> entry:categorymap.entrySet())
				{
					if(entry.getValue()>maxcount)
					{
						maxcount=entry.getValue();
						category=entry.getKey();
					}
				}
				HashMap<String,String> treemap=new HashMap<String,String>();
				System.out.println("单节点"+category+"   "+categorymap);
				String[] st=new String[1];
				st[0]=category;
				treemap.put(currentnodepath, st[0]);
				//writer.append(reduceKey, reduceValue);
			//	writer.close();
				//context.(reduceKey, reduceValue);
				if(fs.exists(pathv))
				{
                    FSDataInputStream fdis=fs.open(pathv);
                    int c=fdis.readInt();
                    for(int i=0;i<c;i++)
                    {
                    	treemap.put(fdis.readUTF(),fdis.readUTF());
                    }
                    fdis.close();
                    treemap.put(currentnodepath, st[0]);
				}
				System.out.println("treemap="+treemap);
				FSDataOutputStream fdos=fs.create(pathv);
				fdos.writeInt(treemap.size());
				for(Map.Entry<String, String> entry:treemap.entrySet())
				{
					fdos.writeUTF(entry.getKey());
					fdos.writeUTF(entry.getValue());
				}
				fdos.close();
			}
		}

		@Override
		protected void reduce(Text arg0, Iterable<ComValue> arg1,Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("arg0="+arg0);
			HashMap<ComV,Integer> map=new HashMap();
			int categorycount=0;
			for(ComValue cv:arg1)
			{
				categorycount++;
				ComV comv=new ComV();
				comv.set(cv.getFirst(),cv.getSecond());
			//	System.out.println(cv);
				if(map.get(cv)==null)
				{
					map.put(comv, 1);
				}
				else
				{
					int count=map.get(cv);
					map.put(comv, count+1);
				}
			}
			System.out.println(map);
			String flag=null;
			for(Map.Entry<ComV, Integer> entry:map.entrySet())
			{
				ComV cv=entry.getKey();
				int count=entry.getValue();
				if(flag==null)
				{
					flag=cv.getFirst();
				}
				if(cv.getFirst().equals(flag))
				{
					if(!categorymap.containsKey(arg0.toString()))
					{
						categorymap.put(arg0.toString(),count);
					}
					else
					{
						categorymap.put(arg0.toString(), categorymap.get(arg0.toString())+count);
					}
				}
				//这个存放 类别和类别对应的个数
				ComV gda=new ComV();
				gda.set(arg0.toString(),count+"");
				ArrayList<ComV> arrayList=null;
				if(!gdamap.containsKey(cv))
				{
					arrayList=new ArrayList();
				}
				else
				{
					arrayList=gdamap.get(cv);
				}
				arrayList.add(gda);
				gdamap.put(cv, arrayList);
			}
		}
	}
	
	//预测mapper
 public static class PredictMapper extends Mapper<LongWritable,Text,Text,Text>
 {
	 //存储路径和类别的对应关系 比如1：青年-》是
	   public HashMap<String,String> treemap=new HashMap();
	   Text mapkey=new Text();
	   Text mapvalue=new Text();
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration() ;
			Path p=new Path(conf.get("HDFS"));
			FileSystem fs = p.getFileSystem ( conf) ;
			Path pathv=new Path(conf.get("trainout")+"/decisiontree");
            FSDataInputStream fsdis=fs.open(pathv);
            int count=fsdis.readInt();
            for(int i=0;i<count;i++)
            {
            	treemap.put(fsdis.readUTF(), fsdis.readUTF());
            }
            fsdis.close();
            System.out.println(treemap);
		}
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] values=value.toString().split(",");
		for(Map.Entry<String, String> entry:treemap.entrySet())
		{
			String[] var=entry.getKey().split(",");
			boolean  flag=true;
			for(int i=0;i<var.length;i++)
			{
				String[] d=var[i].split(":");
				if(!values[Integer.parseInt(d[0])-1].equals(d[1]))
				{
					flag=false;
					break;
				}
			}
			if(flag==true)
			{
				mapkey.set(value.toString());
				mapvalue.set(entry.getValue());
				context.write(mapkey, mapvalue);
				break;
			}
		}
	}
 }
 //预测reducer
  public static class PredictReducer extends Reducer<Text,Text,Text,Text>
  {
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text value:arg1)
		{
			System.out.println(arg0.toString()+"    "+value.toString());
			arg2.write(arg0, value);
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
   	double E=(Double)manager.getParamsValue(timestamp, "E");
   	conf.setFloat("E", (float) E);
   	FileSystem fs=p.getFileSystem(conf);
   	String parentpath=p.toString();
   	
   	trainInputPath=parentpath+manager.getParamsValue(timestamp, "traininputPath");
   	String predictInputPath=parentpath+manager.getParamsValue(timestamp, "predictInputPath");
   	String outputPath=parentpath+"/"+timestamp+manager.getParamsValue(timestamp, "outputPath");
   	//首先程序开始时，设置节点数量为0，这个节点表示将要进行判断的条件
   	String trainOutputPath=parentpath+"/"+timestamp+"/decisionout";
   	String out=parentpath+"/"+timestamp+"/out";
   	String tempout=parentpath+"/"+timestamp+"/decision";
   	fs.delete(new Path(tempout));
   	conf.set("tempout", tempout);
   	conf.set("trainout", trainOutputPath);
   	Path var=new Path(tempout+"/decisionnode");
   	System.out.println(var.toString());
	FSDataOutputStream fsos=fs.create(var);
	fsos.writeInt(0);
	fsos.close();
   //设置当前决策路径
	conf.set("currentnode","");
	conf.set("time", timestamp+"");
   	String predictReadPath=parentpath+manager.getParamsValue(timestamp,"predictInputPath");
   	String predictOutPath=parentpath+"/"+timestamp+manager.getParamsValue(timestamp,"outputPath");
    fs.delete(new Path(predictOutPath));
    fs.delete(new Path(trainOutputPath));
    while(true)
    {
       	fs.delete(new Path(out));
    	Job job1=new Job(conf);
       	job1.setJarByClass(decisiontree.class);
       	job1.setMapperClass(trainMapper.class);
       	job1.setReducerClass(trainReducer.class);
       	job1.setMapOutputKeyClass(Text.class);
       	job1.setMapOutputValueClass(ComValue.class);
       	job1.setOutputKeyClass(Text.class);
       	job1.setOutputValueClass(Text.class);
      // 	job1.setOutputFormatClass ( FileOutputFormat.class ) ;
       	FileInputFormat.addInputPath ( job1 , new Path ( trainInputPath ) ) ;
       	FileOutputFormat.setOutputPath(job1, new Path(out));
       	job1.waitForCompletion(true);
        //进行判断，是否满足终止条件
       	FSDataInputStream fsdis=fs.open(new Path(tempout+"/decisionnode"));
       	int nodecount=fsdis.readInt();
       	System.out.println("nodecount="+nodecount);
       	if(nodecount==0)
       	{
       		break;
       	}
       	//取出一个节点，作为当前节点，可以类比队列
       //	int arraycount=fsdis.readInt();
       	System.out.println("当前个数  "+nodecount);
       	String[] array=new String[nodecount];
       	for(int i=0;i<array.length;i++)
       	{
       		array[i]=fsdis.readUTF();
       	}
       	conf.set("currentnode", array[0]);
       	fs.delete(new Path(tempout+"/decisionnode"));
       	FSDataOutputStream fddos=fs.create(new Path(tempout+"/decisionnode"));
       	fddos.writeInt(nodecount-1);
    	for(int i=1;i<array.length;i++)
       	{
       		fddos.writeUTF(array[i]);
       	}
    	fddos.close();
    }
    //预测job
    Job job2=new Job(conf);
   	job2.setJarByClass(decisiontree.class);
   	job2.setMapperClass(PredictMapper.class);
   	job2.setReducerClass(PredictReducer.class);
   	job2.setMapOutputKeyClass(Text.class);
   	job2.setMapOutputValueClass(Text.class);
   	job2.setOutputKeyClass(Text.class);
   	job2.setOutputValueClass(Text.class);
  // 	job1.setOutputFormatClass ( FileOutputFormat.class ) ;
   	FileInputFormat.addInputPath ( job2 , new Path ( predictInputPath ) ) ;
   	FileOutputFormat.setOutputPath(job2, new Path(outputPath));
   	job2.waitForCompletion(true);
	Path pathv=new Path(conf.get("trainout")+"/decisiontree");
	HashMap<String,String> treemap=new HashMap<String,String>();
	 FSDataInputStream fdis=fs.open(pathv);
     int c=fdis.readInt();
     for(int i=0;i<c;i++)
     {
     	treemap.put(fdis.readUTF(),fdis.readUTF());
     }
     fdis.close();
   	System.out.println("=========="+treemap);
   }
}
