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

public class svd {
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
			 public ArrayList<double[]> trans(ArrayList<double[]>  trans)
			 {
				 ArrayList<double[]> A=new ArrayList<double[]>();
				 for(int i=0;i<trans.get(0).length;i++)
				 {
					 double[] temp=new double[trans.size()];
					 for(int j=0;j<temp.length;j++)
					 {
						 temp[j]=trans.get(j)[i];
					 }
					 A.add(temp);
				 }
				 return A;
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
               
            newA=trans(newA);
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
   //QR分解得到的Q写入文件中,Q以列的形式存储
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
		    context.write(mapkey, mapvalue);
		    index++;
		}	
   }
   //求特征值reducer
   public static class EigenReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>
   {
	   //存储特征值，但是只存储不同的特征值
	ArrayList<Double> eigenlist=new ArrayList();
	DoubleWritable reducevalue=new DoubleWritable();
	@Override
	protected void reduce(IntWritable arg0, Iterable<DoubleWritable> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(DoubleWritable val:arg1)
		{
			reducevalue.set(val.get());
	        if(Math.abs(val.get())<0.00000001)
	        {
	             reducevalue.set(0);
	        }
	        if(!eigenlist.contains(reducevalue.get()))
	        {
	        	arg2.write(null, reducevalue);
	        	eigenlist.add(reducevalue.get());
	        }
		}
	}
   }
   //根据特征值求特征向量，利用的方法是lu分解，其中使用的矩阵是martixmulout中输出的矩阵，然后M-E,DoubleWritable表示矩阵特征值
   public static class LUMapper extends Mapper<LongWritable,Text,DoubleWritable,Text>
   {
	//   double[] eigen=new double[2];
	   Path p;
	   FileSystem fs;
	   ArrayList<Double> eigenlist=new ArrayList();
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
		   Configuration conf=context.getConfiguration();
			 p=new Path(conf.get("HDFS"));
			 fs = p.getFileSystem ( conf) ;
			 FSDataInputStream fsdis=fs.open(new Path(p.toString()+"/home/novas/SVD/eigenout/part-r-00000"));
			 String line=fsdis.readLine();
			 while(line!=null)
			 {
				 eigenlist.add(Double.valueOf(line));
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
		for(int i=0;i<eigenlist.size();i++)
		{
			mapkey.set(eigenlist.get(i));
			mapvalue.set(index+"_"+getString(d));
			context.write(mapkey, mapvalue);
		}
		index++;
	} 
   }
   //计算LU分解reducer,从Mapper中传递过来的特征值，会自动进行排序，从小到大
   public static class LUReducer extends Reducer<DoubleWritable,Text,DoubleWritable,Text>
   {
	   //存储减去特征值之后的矩阵
	   ArrayList<double[]> Alist=new ArrayList();
	   String[] strs=new String[10000];
	   ArrayList<Double> eigenlist=new ArrayList<Double>();
	   Text reducevalue=new Text();
	   Path p;
	   FileSystem fs;
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
      /* 这部分方法暂时舍弃不用
	   //存储U矩阵,LU分解的U矩阵
	   ArrayList<double[]> Ulist=new ArrayList();
	   //矩阵的维数，其中矩阵的行数和列数相等
	   int rows;
	
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
        		   L[j]=(A.get(i)[j]-sum)/Ulist.get(j)[j];
        	   }
        	   L[i]=1.0;
        	   Llist.add(L);
        	   System.out.println("L=========");
        	   for(int p=0;p<Llist.size();p++)
       		{
       			System.out.println(getString(Llist.get(p)));
       		}
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
              System.out.println("U=========");
      		for(int p=0;p<Ulist.size();p++)
      		{
      			System.out.println(getString(Ulist.get(p)));
      		}
           }
           return Ulist;
	   }
	   */
	   //获取两个数组的乘
	   public double getLineMulLine(double[] A,double[] B)
	   {
		   double sum=0;
		   for(int i=0;i<A.length;i++)
		   {
			   sum=sum+A[i]*B[i];
		   }
		   return sum;
	   }
	   //获取两个矩阵乘法的结果，因为都是对称矩阵，所以可以使用行×行相乘和行X列效果相同
	   public ArrayList<double[]> getMartixMul(ArrayList<double[]> A,ArrayList<double[]> B)
	   {
		   ArrayList<double[]> C=new ArrayList<double[]>();
		   for(int i=0;i<A.size();i++)
		   {
			   double[] AA=A.get(i);
			   double[] CC=new double[AA.length];
			   for(int j=0;j<CC.length;j++)
			   {
				   CC[j]=getLineMulLine(AA,B.get(j));
			   }
			   C.add(CC);
		   }
		   return C;
	   }
	   //获取YE-A,其中Y为特征值
       public ArrayList<double[]> getEA(ArrayList<double[]> Alist,double eigen)
       {
    	   ArrayList<double[]> EAlist=new ArrayList();
    	   for(int i=0;i<Alist.size();i++)
    	   {
    		   double[] A=Alist.get(i);
    		   double[] EA=new double[A.length];
    		   for(int j=0;j<EA.length;j++)
    		   {
    			   if(i==j)
    			   {
    				   EA[j]=eigen-A[j];
    			   }
    			   else
    			   {
    				   EA[j]=-A[j];
    			   }
    		   }
    		   EAlist.add(EA);
    	   }
    	   return EAlist;
       }
       //将特征向量变成单位向量
       public double[] getOneVector(double[] vector)
       {
    	   double sum=0;
    	   for(int i=0;i<vector.length;i++)
    	   {
    		   sum=sum+vector[i]*vector[i];
    	   }
    	   sum=Math.sqrt(sum);
    	   double[] res=new double[vector.length];
    	   for(int i=0;i<vector.length;i++)
    	   {
    		   res[i]=vector[i]/sum;
    	   }
    	   return res;
       }
	   //获取特征值，根据的算法是如果一个矩阵的特征值为3,1,0这三个不同的特征值，那么3对应的特征向量是(1*E-A)(0*E-A)
	   //同理，1对应的特征向量是(3*E-A)(0*E-A)
	   public String  getVector(ArrayList<Double> eigenlist,ArrayList<double[]> Alist,double currenteigen)
	   {
		   ArrayList<double[]> templist=new ArrayList<double[]>();
		   for(int i=0;i<Alist.size();i++)
		   {
			   double[] temp=new double[Alist.size()];
			   temp[i]=1;
			   templist.add(temp);
		   }
		   //依次遍历所有特征值，对于不等于currenteigen的特征值，进行乘法计算
		   for(int i=0;i<eigenlist.size();i++)
		   {
			   if(eigenlist.get(i)!=currenteigen)
			   {
				   ArrayList<double[]> B=getEA(Alist,eigenlist.get(i));
				   templist=getMartixMul(templist,B);
			   }
		   }
		   //第0行就是所求的特征向量，进行单位化，模长为1
          double[] res=getOneVector(templist.get(0));
          return getString(res);
	   }
	   @Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		   //eigenlist.add(3.0);
		   //eigenlist.add(1.0);
		  // eigenlist.add(0.0);
		   Configuration conf=context.getConfiguration();
			 p=new Path(conf.get("HDFS"));
			 fs = p.getFileSystem ( conf) ;
			 FSDataInputStream fsdis=fs.open(new Path(p.toString()+"/home/novas/SVD/eigenout/part-r-00000"));
			 String line=fsdis.readLine();
			 while(line!=null)
			 {
				 eigenlist.add(Double.valueOf(line));
				 line=fsdis.readLine();
			 }
			 fsdis.close();
		}
	@Override
	protected void reduce(DoubleWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("eigen="+arg0.get());
		double currenteigen=arg0.get();
		int count=0;
		for(Text val:arg1)
		{
			String[] var=val.toString().split("_");
			System.out.println("var="+val);
			strs[Integer.parseInt(var[0])]=var[1];
			count++;
		}
		//生成A矩阵，然后进行LU分解
		Alist.clear();
		for(int i=0;i<count;i++)
		{
			Alist.add(getArray(strs[i]));
		}
	//	rows=Alist.size();
		//进行LU分解
	//	Ulist=LU(Alist);
 	 String vector=getVector(eigenlist,Alist,currenteigen);
 	 System.out.println("vector="+vector);
 	 reducevalue.set(vector);
 	 //0特征值的特征向量不考虑
 	 if(arg0.get()!=0)
 	 arg2.write(arg0,reducevalue);
	}
   }
   
   //当qr分解得到的特征值不是很满意的时候，那么可以进行迭代，其中，qrout是以hessenbergout为输入的，新的输入为上一次
   //qr分解得到的RQ的值，所以，输入设为qrout,输出的是R的结果
   public static class NewHessenbergMapper extends Mapper<LongWritable,Text,IntWritable,Text>
   {
	   Path p;
	   FileSystem fs;
	   //存储Q，Q用列的形式存储
	   ArrayList<double[]> Qlist=new ArrayList();
	   int index=0;
	   IntWritable mapkey=new IntWritable();
	   Text mapvalue=new Text();
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
	   public double getLineMulLine(double[] A,double[] B)
	   {
		   double sum=0;
		   for(int i=0;i<A.length;i++)
		   {
			   sum=sum+A[i]*B[i];
		   }
		   return sum;
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
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		double[] R=getArray(value.toString());
		double[] temp=new double[R.length];
		for(int i=0;i<temp.length;i++)
		{
			temp[i]=getLineMulLine(R,Qlist.get(i));
		}
		mapkey.set(index);
		mapvalue.set(getString(temp));
		context.write(mapkey, mapvalue);
	}   
   }
   //写入新的hessenberg矩阵
   public static class NewHessenbergReducer extends Reducer<IntWritable,Text,Text,Text>
   {
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text val:arg1)
		{
			arg2.write(null, val);
		}
	}
   }
   //计算U矩阵，U=A*V*P-1，P-1是奇异值矩阵的逆矩阵,其中，A是input中的矩阵，最终svd分解的样式为A=U*sigma*VH。
   public static class UMapper extends Mapper<LongWritable,Text,IntWritable,Text>
   {
       Path p;
       FileSystem fs;
       //存储V和奇异值矩阵的逆的乘积
       ArrayList<double[]> vsigmalist=new ArrayList<double[]>();
       //存储奇异值，也就是sigma矩阵
       ArrayList<Double> sigmalist=new  ArrayList();
       //Vh矩阵
       ArrayList<String> vhlist=new ArrayList<>();
       //index表示第几行
       int index=0;
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
	   //获取两个数组的乘
	   public double getLineMulLine(double[] A,double[] B)
	   {
		   double sum=0;
		   for(int i=0;i<A.length;i++)
		   {
			   sum=sum+A[i]*B[i];
		   }
		   return sum;
	   }
	   //在setup中，计算V*P-1,其中，奇异值为特征值的开根号，P-1 除对角线以外都是0，所以V的特征值和特征向量的对应关系，保存在
	   //eigenvectorout中，将特征向量和奇异值的倒数相乘即可
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf=context.getConfiguration();
			 p=new Path(conf.get("HDFS"));
			 fs = p.getFileSystem ( conf) ;
			 FSDataInputStream fsdis=fs.open(new Path(p.toString()+"/home/novas/SVD/eigenvectorout/part-r-00000"));
			 String line=fsdis.readLine();
			 while(line!=null)
			 {
				 String[] var=line.split("\t");
				 double eigen=Double.valueOf(var[0]);
				 double[] q=getArray(var[1]);
				 System.out.println("Q="+var[1]);
				 vhlist.add(var[1]);
				 for(int i=0;i<q.length;i++)
				 {
					 q[i]=q[i]/Math.sqrt(eigen);
				 }
				 sigmalist.add(Math.sqrt(eigen));
				 vsigmalist.add(q);
				 line=fsdis.readLine();
			 }
			 fsdis.close();
		}
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
	    double[] A=getArray(value.toString());
	    double[] res=new double[vsigmalist.size()];
	    for(int i=0;i<vsigmalist.size();i++)
	    {
	    	res[i]=getLineMulLine(A,vsigmalist.get(i));
	    }
	    mapkey.set(index);
	    mapvalue.set(getString(res));
	    context.write(mapkey, mapvalue);
	    index++;
	}
	//将奇异值矩阵写入文件中,同时，将VH也写入文件
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FSDataOutputStream fsdos=fs.create(new Path(p.toString()+"/home/novas/SVD/SIGMA/sigma.martix"));
		for(int i=0;i<sigmalist.size();i++)
		{
			double[] line=new double[sigmalist.size()];
			line[i]=sigmalist.get(i);
			fsdos.writeBytes(getString(line)+"\r\n");
		}
		fsdos.close();
		//写入转置矩阵
		 fsdos=fs.create(new Path(p.toString()+"/home/novas/SVD/VH/vh.martix"));
		for(int i=0;i<vhlist.size();i++)
		{
			fsdos.writeBytes(vhlist.get(i)+"\r\n");
		}
		fsdos.close();
		
	}
   }
   //将得到的矩阵写入文件
   public static class UReducer extends Reducer<IntWritable,Text,Text,Text>
   {
	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text val:arg1)
		{
			arg2.write(null, val);
		}
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
		  String eigenvectorout=parentpath+"/home/novas/SVD/eigenvectorout";
		  String uout=parentpath+"/home/novas/SVD/U";
		  fs.delete(new Path(uout));
		  fs.delete(new Path(out));
		  fs.delete(new Path(transmartixout));
		  fs.delete(new Path(martixmulout));
		  fs.delete(new Path(hessenbergout));
		  fs.delete(new Path(qrout));
		  fs.delete(new Path(eigenout));
		  fs.delete(new Path(eigenvectorout));
//求矩阵转置的job
		  Job transjob = new Job ( conf ) ;
		  transjob.setJarByClass(svd.class);
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
	 	 //求矩阵的转置和矩阵乘 AhA,并且计算了hessenberg矩阵
	 	  Job muljob = new Job ( conf ) ;
	 	 muljob.setJarByClass(svd.class);
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
	 	 int count=0;
	 	 while(true)
	 	 {
	 		  //qr分解
			  Job qrjob = new Job ( conf ) ;
			  qrjob.setJarByClass(svd.class);
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
			  newhessenbergjob.setJarByClass(svd.class);
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
		  eigenjob.setJarByClass(svd.class);
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
	     eigenvectorjob.setJarByClass(svd.class);
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
	     ujob.setJarByClass(svd.class);
	     ujob.setMapperClass ( UMapper.class ) ;
	     ujob.setReducerClass ( UReducer.class ) ;
	     ujob.setMapOutputKeyClass(IntWritable.class);
	     ujob.setMapOutputValueClass(Text.class);
	     ujob.setOutputKeyClass ( Text.class);
	     ujob.setOutputValueClass ( Text.class);
	 	 FileInputFormat.addInputPath ( ujob , new Path ( readPath ) ) ;
	 	 FileOutputFormat.setOutputPath ( ujob ,  new  Path ( uout ) ) ;
	 	ujob.waitForCompletion ( true ) ;
   }
}
