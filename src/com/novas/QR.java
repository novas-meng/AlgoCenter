package com.novas;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

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
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.novas.fcm.RandomMartixMapper;
import com.novas.fcm.RandomMartixReducer;

public class QR {
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
	//计算矩阵AHA，矩阵A的转置和矩阵A的成绩，最终的结果以列的形式存储
	public static class MartixMulMapper extends Mapper<LongWritable,Text, IntWritable,Text>
	{

		int count=0;
		IntWritable mapkey=new IntWritable();
		Text mapvalue=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			Path p=new Path(conf.get("HDFS"));
			FileSystem fs = p.getFileSystem ( conf) ;
			Path var1=new Path(p.toString()+"/home/novas/SVD/transout/part-r-00000");
			FSDataInputStream fdis=fs.open(var1);
			System.out.println("value="+value.toString());
			String[] var2=value.toString().split(",");
			String line=fdis.readLine();
			int linecount=0;
			while(line!=null)
			{
				System.out.println("line="+line);
				String[] var3=line.split(",");
				double sum=0;
				for(int i=0;i<var2.length;i++)
				{
					sum=sum+Double.parseDouble(var2[i])*Double.parseDouble(var3[i]);
				}
				mapkey.set(linecount);
				mapvalue.set(count+"_"+sum);
				context.write(mapkey,mapvalue);
				linecount++;
				line=fdis.readLine();
			}
			count++;
		}
	}
	
	public static class MartixMulReducer extends Reducer<IntWritable,Text,IntWritable,Text>
	{
        IntWritable reducekey=new IntWritable();
        Text reducevalue=new Text();
        ArrayList<double[]> A=new ArrayList();
        String[] strs=new String[10000];
         int linecount=0;
        FileSystem fs;
		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			linecount++;
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
			A.add(getArray(reducevalue.toString()));
		}
		//根据字符串获取对应数组
		 public static double[] getArray(String var1)
		   {
			    String[] var3=var1.split(",");
			    double[] var4=new double[var3.length];
			    for(int i=0;i<var4.length;i++)
			    {
			    	var4[i]=Double.valueOf(var3[i]);
			    } 
			    return var4;
		   }
		 //获取X向量
		 public double[]  getX(double[] line,int i)
		 {
			 double[] res=new double[line.length];
			 System.arraycopy(line,0,res,0, line.length);
			 for(int j=0;j<i+1;j++)
			 {
				 res[j]=0;
			 }
			 return res;
		 }
		 //获取W向量 
		 public double[] getW(double[] x,int index)
		 {
			 double sum=0;
			 double[] W=new double[x.length];
			 for(int i=0;i<x.length;i++)
			 {
				 sum=sum+x[i]*x[i];
			 }
			 W[index+1]=Math.sqrt(sum);
			 return W;
		 }
		 //获取V向量
		 public double[] getV(double[] X,double[] W)
		 {
			 double[] V=new double[X.length];
			 int sign=1;
			 if(X[0]>0)
			 {
				 sign=-1;
			 }
			 for(int i=0;i<V.length;i++)
			 {
				 V[i]=W[i]+sign*X[i];
			 }
			 return V;
		 }
		 //计算VHV 
		 public double getVHV(double[] d)
		 {
			 double sum=0;
			 for(int i=0;i<d.length;i++)
			 {
				 sum=sum+d[i]*d[i];
			 }
			 return sum;
		 }
		 //计算H
		 public ArrayList<double[]> getH(double[] V,double VHV)
		 {
			ArrayList<double[]> H=new ArrayList();
			 for(int i=0;i<V.length;i++)
			 {
				 double[] temp=new double[V.length];
				 for(int j=0;j<temp.length;j++)
				 {
					 if(i==j)
					 {
						 temp[j]=1-2*V[i]*V[j]/VHV;
					 }
					 else
					 {
						 temp[j]=-2*V[i]*V[j]/VHV;
					 }
				 }
				 H.add(temp);
			 }
			 return H;
		 }
		 //获取矩阵乘法后新的一行
		 public double[] getNewLine(double[] H,ArrayList<double[]> A)
		 {
			 double[] newline=new double[H.length];
			 for(int i=0;i<newline.length;i++)
			 {
				 double[] var=A.get(i);
				 double sum=0;
				 for(int j=0;j<H.length;j++)
				 {
					 sum=sum+H[j]*var[j];
				 }
				 newline[i]=sum;
			 }
			 return newline;
		 }
		//在clean中，计算hessenberg矩阵
		 //把数组转换为String
		   public static String getString(double[] d)
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
		   //将得到的hessenberg矩阵写入hdfs
		   public void writeToFile(ArrayList<double[]> A,FileSystem fs,Path p) throws IOException
		   {
			   Path hessbergpath=new Path(p.toString()+"/home/novas/SVD/hessenbergout/hessenberg.martix");
			   FSDataOutputStream fdos=fs.create(hessbergpath);
			   for(int i=0;i<A.size();i++)
			   {
				   double[] d=A.get(i);
				   for(int j=0;j<d.length;j++)
				   {
					   if(Math.abs(d[j])<0.000000000001)
					   {
						  d[j]=0;
					  }
				   }
				   String line=getString(d);
				   fdos.writeBytes(line+"\r\n");
			   }
			   fdos.close();
		   }
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		//	super.cleanup(context);
			Configuration conf=context.getConfiguration();
			Path p=new Path(conf.get("HDFS"));
			fs = p.getFileSystem ( conf) ; 
			System.out.println("A="+A.size()+"   "+A);
			ArrayList<double[]> H=new ArrayList();
			ArrayList<double[]> newA=new ArrayList();
			//使用总列数-2 列就可以了
			
			for(int i=0;i<linecount-2;i++)
			{
				double[] line=A.get(i);
				//计算X
				double[] X=getX(line,i);
				//计算W
				double[] W=getW(X,i);
				//计算V
				double[] V=getV(X,W);
				//		//计算H ，其中H=1-2P,P=V*Vh/Vh*V
               double VHV=getVHV(V);
               H=getH(V,VHV);
               System.out.println("H=====");
        //   	for(int m=0;m<H.size();m++)
		//	{
			//	double[] temp=H.get(m);
			//	for(int j=0;j<temp.length;j++)
		//		{
			//		System.out.print(temp[j]+"  ");
		//		}
		//		System.out.println();
		//	}
               newA=new ArrayList();
               System.out.println("H.size="+H.size());
               for(int j=0;j<H.size();j++)
               {
            	   double[] H1=H.get(j);
            	   double[] temp=getNewLine(H1,A);
            	   double[] newLine=getNewLine(temp,H);
            	   System.out.println("==='"+getString(newLine));
            	   newA.add(newLine);
              }
	    	A.clear();
			A.addAll(newA);
		//	System.out.println("hessenberg="+A.size());
			}
			//将得到的Hessenberg矩阵写入结果
			writeToFile(A,fs,p);
		}
	}
	
	
	//计算hessenberg矩阵,Mapper中输出的是H1，
  public static class HessenbergMapper extends Mapper<LongWritable,Text,IntWritable,Text>
  {
	 
	int count=0;
	IntWritable mapkey=new IntWritable();
	Text mapvalue=new Text();
	int Hcount=-1;
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//计算X
		String[] var=value.toString().split(",");
		if(Hcount==-1)
		{
			Hcount=var.length;
		}
		//只需要列数-2就够用了
		if(count>Hcount-2)
		{
			return;
		}
		for(int i=0;i<=count;i++)
		{
			var[i]="0";
		}
		//计算W
		double sum=0;
		double temp=Double.parseDouble(var[count+1]);
		for(int i=0;i<var.length;i++)
		{
			sum=sum+Double.valueOf(var[i])*Double.valueOf(var[i]);
		}
		//计算V=W+sgn(X) 如果x0>0 sgn=-1 x0<0 sgn=1；
		if(Double.valueOf(var[0])>0)
		{
			//sgn=-1
			for(int i=0;i<var.length;i++)
			{
				var[i]=-1*Double.valueOf(var[i])+"";
			}
			var[count+1]= -1 * Double.parseDouble(var[count+1])+Math.sqrt(sum)+"";
			temp=temp*-1;
		}
		else
		{
			var[count+1]=Double.parseDouble(var[count+1])+Math.sqrt(sum)+"";
		}
		//计算H ，其中H=1-2P,P=V*Vh/Vh*V
		sum=sum+sum+2*temp*Math.sqrt(sum);
		for(int i=0;i<var.length;i++)
		{
			StringBuilder sb=new StringBuilder();
			for(int j=0;j<var.length;j++)
			{
				double m=Double.valueOf(var[i])*Double.valueOf(var[j])/sum;
				if(i==j)
				{
					m=1-2*m;
				}
				else
				{
					m=-2*m;
				}
				if(j!=var.length-1)
				{
					sb.append(m+",");
				}
				else
				{
					sb.append(m+"");
				}
			}
			mapkey.set(count);
			mapvalue.set(i+"_"+sb.toString());
			context.write(mapkey, mapvalue);
		}
		count++;
	}
  }
  public static class HessenbergReducer extends Reducer<IntWritable,Text,Text,Text>
  {
	   FileSystem fs;
	   String[] strs=new String[10000];
	   Path copy;
	   Path p;
	   public static String getString(double[] d)
	   {
		   StringBuilder sb=new StringBuilder();
		   int i;
		   for( i=0;i<d.length-1;i++)
		   {
			   sb.append(d[i]+",");
		   }
		   sb.append(d[i]+"\r\n");
		   return sb.toString();
	   }
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			 p=new Path(conf.get("HDFS"));
			fs = p.getFileSystem ( conf) ;
			copy=new Path(p.toString()+"/home/novas/SVD/hessenbergout/martixmulcopy.data");
			FSDataOutputStream fsdos=fs.create(copy);
			FSDataInputStream fsdis=fs.open(new Path(p.toString()+"/home/novas/SVD/martixmulout/part-r-00000"));
			byte[] bytes=new byte[1024*1024];
			int len=0;
			while((len=fsdis.read(bytes))!=-1)
			{
				fsdos.write(bytes, 0, len);
			}
			fsdis.close();
			fsdos.close();
		}
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("hessenberg="+arg0.toString());
		//count表示一共有多少行
		int count=0;
		for(Text val:arg1)
		{
			System.out.println(val);
			String[] var=val.toString().split("_");
			strs[Integer.parseInt(var[0])]=var[1];
			count++;
		}
		FSDataInputStream fdis=fs.open(copy);
		Path newA=new Path(p.toString()+"/home/novas/SVD/hessenbergout/temp.data");
		FSDataOutputStream fdos=fs.create(newA);
		//计算H*A*H,结果保存到copy路径中
		for(int i=0;i<count;i++)
		{
			String[] var1=strs[i].split(",");
			double temp[]=new double[var1.length];
			String line=fdis.readLine();
			System.out.println("line="+line);
			int index=0;
			while(line!=null)
			{
				String[] var2=line.split(",");
				double sum=0;
				for(int j=0;j<var1.length;j++)
				{
					sum=Double.parseDouble(var1[j])*Double.parseDouble(var2[j]);
				}
				temp[index]=sum;
				index++;
				line=fdis.readLine();
			}
			
			double[] newline=new double[var1.length];
			for(int j=0;j<count;j++)
			{
				System.out.println("strs[j]="+strs[j]);
				String[] var3=strs[j].split(",");
				
				double sum=0;
				for(int m=0;m<var3.length;m++)
				{
					sum=Double.parseDouble(var3[m])*temp[m];
				}
				newline[j]=sum;
			}
			fdis.close();
			fdis=fs.open(copy);
			//将新的一行写入
			fdos.write(getString(newline).getBytes());
		}
		fdis.close();
		fdos.close();
		fs.delete(copy);
    	fs.rename(newA, copy);
	} 
  }
  //计算QR分解
   public static class QRMapper extends Mapper<LongWritable,Text,IntWritable,Text>
   {
   int count=0;
   Text temp=new Text();
   IntWritable qrmapkey=new IntWritable();
   Text qrmapvalue=new Text();
   
	@Override
  protected void setup(Context context) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
		qrmapkey.set(-1);
}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key==null)
		{
			//String[] args=qrmapkey.toString().split(",");
		//	args[0]=Integer.parseInt(args[0])+1+"";
			qrmapkey.set(count);
			//System.out.println("qr"+qrmapkey.toString()+"   "+temp.toString());
			context.write(qrmapkey, temp);
          //  return;
		}
		else
		{
			if(temp.getLength()==0)
			{
				temp.set(value.toString());
				//System.out.println("========");
			}
			else
			{
				//String[] var1=temp.toString().split(",");
				//String[] var2=value.toString().split(",");
			//	double var3=Double.valueOf(var1[count]);
			//	//double var4=Double.valueOf(var2[count]);
			//	double cos=var3/Math.sqrt(var3*var3+var4*var4);
			//	double sin=var4/Math.sqrt(var3*var3+var4*var4);
		     	qrmapkey.set(count);
			//	System.out.println("cos="+cos+"sin="+sin+"var4="+var4+"var3="+var3);
		    	context.write(qrmapkey, temp);
			//	Text var6=new Text();
				//var6.set(count+",sin,"+cos+","+sin+","+(count+1));
	           // context.write(var6, value);
	            temp.set(value.toString());
	            count++;
			}
		}
	
	}
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.map(null, temp, context);
	}
   }
   //QR分解的Reducer
   public static class QRReducer extends Reducer<IntWritable,Text,IntWritable,Text>
   {
   Text qrreducervalue=new Text();
   IntWritable tempkey=new IntWritable();
   Text tempvalue=new Text();
   int count=0;
   //存储Q
   ArrayList<double[]> Qlist=new ArrayList();
   //存储Q某一列的暂存
   double[] Qtemp; 
   Path p;
   FileSystem fs;
   //QR分解得到的Q写入文件中
   public void writeToFile(ArrayList<double[]> A,FileSystem fs,Path p) throws IOException
   {
	   Path hessbergpath=new Path(p.toString()+"/home/novas/SVD/Q/Q.martix");
	   FSDataOutputStream fdos=fs.create(hessbergpath);
	   for(int i=0;i<A.size();i++)
	   {
		   double[] d=A.get(i);
		   for(int j=0;j<d.length;j++)
		   {
			   if(Math.abs(d[j])<0.000000000001)
			   {
				  d[j]=0;
			  }
		   }
		   String line=getString(d);
		   fdos.writeBytes(line+"\r\n");
	   }
	   fdos.close();
   }
   @Override
protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
		throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	   tempkey.set(-1);
}
//将Text转化为数组
   public static double[] getArray(Text var1)
   {
	    String[] var3=var1.toString().split(",");
	    double[] var4=new double[var3.length];
	    for(int i=0;i<var4.length;i++)
	    {
	    	var4[i]=Double.valueOf(var3[i]);
	    } 
	    return var4;
   }
   //把数组转换为String
   public static String getString(double[] d)
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
   //添加Q的列
   public  void addQ(double cos,double sin)
   {
	   int size=Qlist.size();
	   //Qtemp 相当于第i列，Q相当于第i+1 列，首先将第I列复制到第I+1列中
	   double[] Q=new double[Qtemp.length];
	   for(int i=0;i<Q.length;i++)
	   {
		   Q[i]=Qtemp[i];
	   }
	   //第I列乘cos，第i+1列乘   -sin
	   for(int i=0;i<Q.length;i++)
	   {
		   Qtemp[i]=Qtemp[i]*cos;
		   Q[i]=Q[i]*sin*-1;
	   }
	   //Qtemp的第size+1 行是sin，Q的第size+1行是cos
	   Qtemp[size+1]=sin;
	   Q[size+1]=cos;
	   System.out.println("i+1    "+getString(Q));
	   double[] newQ=new double[Q.length];
	   System.arraycopy(Qtemp, 0, newQ, 0, Qtemp.length);
	   Qlist.add(newQ);
	   System.arraycopy(Q, 0, Qtemp, 0, Q.length);
   }
   //获得变化后的新的Text，具体算法是，第K行，前k-1个都是0，以后都是cos*Ak,k+sin*AK=1,k,cos*Ak,k+1+sin*Ak+1,k+1
   //第k+1行 前k个是0,然后是 -sin*Ak,k+1+cos*Ak+1,k+1
   public  String getNewText(IntWritable key,Text var1,Text var2)
   {
	   System.out.println("var1="+var1.toString());
	   System.out.println("var2="+var2.toString());
	  System.out.println("key="+key.toString());

	//   String[] strs=key.toString().split(",");
	   int count=key.get();
	   double[] var3=getArray(var1);
	   double[] var4=getArray(var2);
	   double[] var_3=new double[var3.length];
	   double[] var_4=new double[var4.length];
	   System.arraycopy(var3,0,var_3,0, var3.length);
	   System.arraycopy(var4,0,var_4,0, var4.length);
	   
		double cos=var3[count]/Math.sqrt(var3[count]*var3[count]+var4[count]*var4[count]);
		double sin=var4[count]/Math.sqrt(var3[count]*var3[count]+var4[count]*var4[count]);
		if(var4[count]==0)
		{
			sin=0;
			cos=1;
		}
		System.out.println("cos="+cos+"sin="+sin+"var4="+var4[count]+"var3="+var3[count]);
       //添加Q的一列
		addQ(cos,sin);
	   for(int i=0;i<count;i++)
	   {
		   var3[i]=0;
	   }
	   for(int i=count;i<var3.length;i++)
	   {
		   var3[i]=cos*var_3[i]+sin*var_4[i];
	   }
	   for(int i=0;i<=count;i++)
	   {
		   var4[i]=0;
	   }
	   for(int i=count+1;i<var4.length;i++)
	   {
		   var4[i]=-sin*var_3[i]+cos*var_4[i];
	   }
	   var1.set(getString(var4));
	   
	return getString(var3);
   }
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stu
		Configuration conf=arg2.getConfiguration();
		 p=new Path(conf.get("HDFS"));
		fs = p.getFileSystem ( conf) ;
		System.out.println(arg0);
		if(arg0==null)
		{
			 System.out.println("==========="+count);
			 arg2.write(null, tempvalue);
			 return;
		}
		//初始化
		if(tempkey.get()==-1)
		{
			tempkey.set(arg0.get());
			Iterator it=arg1.iterator();
			tempvalue.set(((Text)it.next()).toString());
			System.out.println("tempvalue="+tempvalue);
			Qtemp=new double[tempvalue.toString().split(",").length];
			Qtemp[0]=1;
		} 
		else
		{
			  Iterator it=arg1.iterator();
			 String s= getNewText(tempkey,tempvalue,(Text)it.next());
			 qrreducervalue.set(s);
			 arg2.write(null, qrreducervalue);
			 tempkey.set(arg0.get());
			 count++;
		}
	}
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<Text> list=new ArrayList<Text>();
		list.add(tempvalue);
		reduce(null,list,context);
		System.out.println("Q=======");
		Qlist.add(Qtemp);
		for(int i=0;i<Qlist.size();i++)
		{
			System.out.println(getString(Qlist.get(i)));
		}
		//Qlist中，Q的存储形式是一列一列的存储
		writeToFile(Qlist,fs,p);
	}
   }
   //求特征值
   public static class EigenMapper extends Mapper<LongWritable,Text,IntWritable,DoubleWritable>
   {
//在SetUp中，将Q读取  A=RQ;
	   Path p;
	   FileSystem fs;
	   ArrayList<double[]> Qlist=new ArrayList();
	   int index=0;
	   IntWritable mapkey=new IntWritable();
	   DoubleWritable mapvalue=new DoubleWritable();
	 //将Text转化为数组
	   public static double[] getArray(String var1)
	   {
		    String[] var3=var1.split(",");
		    double[] var4=new double[var3.length];
		    for(int i=0;i<var4.length;i++)
		    {
		    	var4[i]=Double.valueOf(var3[i]);
		    } 
		    return var4;
	   }
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			 p=new Path(conf.get("HDFS"));
			 fs = p.getFileSystem ( conf) ;
			 FSDataInputStream fsdis=fs.open(new Path(p.toString()+"/home/novas/SVD/Q/Q.martix"));
			 String line=fsdis.readLine();
			 while(line!=null)
			 {
				 double[] q=getArray(line);
				 Qlist.add(q);
				 line=fsdis.readLine();
			 }
			 fsdis.close();
		}
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double[] d=getArray(value.toString());
			double[] m=Qlist.get(index);
			double sum=0;
		    for(int i=0;i<d.length;i++)
		    {
		    	sum=sum+d[i]*m[i];
		    }
		    mapkey.set(index);
		    mapvalue.set(sum);
		    System.out.println("fasfasdfadsf");
		    context.write(mapkey, mapvalue);
		    index++;
		}	
   }
   //求特征值reducer
   public static class EigenReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>
   {
	
	@Override
	protected void reduce(IntWritable arg0, Iterable<DoubleWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(DoubleWritable val:arg1)
		{
			arg2.write(arg0, val);
		}
	}
   }
   //根据特征值求特征向量，利用的方法是lu分解，其中使用的矩阵是martixmulout中输出的矩阵，然后M-E,DoubleWritable表示矩阵特征值
   public static class LUMapper extends Mapper<LongWritable,Text,DoubleWritable,Text>
   {
	   double[] eigen=new double[2];
	   int index=0;
	   DoubleWritable mapkey=new DoubleWritable();
	   Text  mapvalue=new Text();
	   //将Text转化为数组
	   public static double[] getArray(String var1)
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
	   public static String getString(double[] d)
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
	   @Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			eigen[0]=3;
			eigen[1]=1;
		}
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		double[] d=getArray(value.toString());
		for(int i=0;i<eigen.length;i++)
		{
			d[index]=d[index]-eigen[i];
			mapkey.set(eigen[i]);
			mapvalue.set(index+"_"+getString(d));
			context.write(mapkey, mapvalue);
			d[index]=d[index]+eigen[i];
		}
		index++;
	} 
   }
   //计算LU分解reducer
   public static class LUReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text>
   {
	   //存储减去特征值之后的矩阵
	   ArrayList<double[]> Alist=new ArrayList();
	   String[] strs=new String[10000];
	 
	   //存储U矩阵,LU分解的U矩阵
	   ArrayList<double[]> Ulist=new ArrayList();
	   //矩阵的维数，其中矩阵的行数和列数相等
	   int rows;
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
	   public ArrayList<double[]> LU(ArrayList<double[]> A)
	   {
		   //L矩阵，以列的形式存储
		   ArrayList<double[]> Llist=new ArrayList();
		   //U矩阵，以列的形式存储
		   ArrayList<double[]> Ulist=new ArrayList();
           for(int i=0;i<A.size();i++)
           {
        	   //计算L
        	   double[] L=new double[A.size()];
        	   L[0]=A.get(i)[0]/A.get(0)[0];
        	   for(int j=1;j<i;j++)
        	   {
        		   double sum=0;
        		   for(int k=0;k<j;k++)
        		   {
        			   sum=sum+L[k]*Ulist.get(k)[j];
        		   }
        		   L[j]=A.get(i)[j]-sum;
        	   }
        	   L[i]=1.0;
        	   Llist.add(L);
        	   //计算U
        	   double[] U=new double[A.size()];
        	   for(int j=i;j<A.size();j++)
        	   {
        		   double sum=0;
        		   for(int k=0;k<i;k++)
        		   {
        			   sum=sum+Llist.get(i)[k]*Ulist.get(k)[j];
        		   }
        		   U[j]=A.get(i)[j]-sum;
        	   }
              Ulist.add(U);
           }
           return Ulist;
	   }
	@Override
	protected void reduce(DoubleWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("eigen="+arg0.get());
		int count=0;
		for(Text val:arg1)
		{
			String[] var=val.toString().split("_");
			strs[Integer.parseInt(var[0])]=var[1];
			count++;
		}
		//生成A矩阵，然后进行LU分解
		for(int i=0;i<count;i++)
		{
			Alist.add(getArray(strs[i]));
		}
		rows=Alist.size();
		//进行LU分解
		Ulist=LU(Alist);
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
		  readPath=parentpath+"/home/novas/SVD/input";
		  String out=parentpath+"/home/novas/SVD/out";
		  //转置矩阵输出路径
		  String transmartixout=parentpath+"/home/novas/SVD/transout";
		  //转置矩阵和矩阵乘输出路径
		  String martixmulout=parentpath+"/home/novas/SVD/martixmulout";
		  //hessenberg矩阵输出路径
		  String hessenbergout=parentpath+"/home/novas/SVD/hessenbergout";
		  String qrout=parentpath+"/home/novas/SVD/qrout";
		  String eigenout=parentpath+"/home/novas/SVD/eigenout";
		  String eigenvector=parentpath+"/home/novas/SVD/eigenvector";
		  String eigenvectorout=parentpath+"/home/novas/SVD/eigenvectorout";
		  fs.delete(new Path(out));
		  fs.delete(new Path(transmartixout));
		  fs.delete(new Path(martixmulout));
		  fs.delete(new Path(hessenbergout));
		  fs.delete(new Path(qrout));
		  fs.delete(new Path(eigenout));
//求矩阵转置的job
		  Job transjob = new Job ( conf ) ;
		  transjob.setJarByClass(QR.class);
		 //设定一些常量参数
		  transjob.setMapperClass ( TransMapper.class ) ;
		  transjob.setReducerClass ( TransReducer.class ) ;
		  transjob.setMapOutputKeyClass(IntWritable.class);
		  transjob.setMapOutputValueClass(Text.class);
		  transjob.setOutputKeyClass ( IntWritable.class);
		  transjob.setOutputValueClass ( Text.class);
	 	
	 	 FileInputFormat.addInputPath ( transjob , new Path ( readPath ) ) ;
	 	 FileOutputFormat.setOutputPath ( transjob ,  new  Path ( transmartixout ) ) ;
	 	 transjob.waitForCompletion ( true ) ;
	 	 //求矩阵的转置和矩阵乘 AhA
	 	  Job muljob = new Job ( conf ) ;
	 	 muljob.setJarByClass(QR.class);
		 //设定一些常量参数
	 	muljob.setMapperClass (MartixMulMapper .class ) ;
	 	muljob.setReducerClass ( MartixMulReducer.class ) ;
	 	muljob.setMapOutputKeyClass(IntWritable.class);
	 	muljob.setMapOutputValueClass(Text.class);
	 	muljob.setOutputKeyClass ( IntWritable.class);
	 	muljob.setOutputValueClass ( Text.class);
	 	
	 	 FileInputFormat.addInputPath ( muljob , new Path ( transmartixout ) ) ;
	 	 FileOutputFormat.setOutputPath ( muljob ,  new  Path ( martixmulout ) ) ;
	 	 muljob.waitForCompletion ( true ) ;
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
	 	 FileInputFormat.addInputPath ( eigenvectorjob , new Path ( eigenvector ) ) ;
	 	 FileOutputFormat.setOutputPath ( eigenvectorjob ,  new  Path ( eigenvectorout ) ) ;
	     eigenvectorjob.waitForCompletion ( true ) ;
   }
}
