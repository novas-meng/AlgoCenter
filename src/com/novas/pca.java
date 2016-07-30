package com.novas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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


public class pca implements Algo
{
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
			String transout=conf.get("transout");
			Path var1=new Path(transout+"/part-r-00000");
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
		ArrayList<double[]> A=new ArrayList();
	    int linecount=0;
	    FileSystem fs;
	    String hessenbergout;
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
		protected void reduce(IntWritable arg0, Iterable<Text> arg1,
				Context arg2) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			linecount++;
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
			A.add(getArray(sb.toString()));
			arg2.write(null, reducevalue);
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
		   //将得到的hessenberg矩阵写入hdfs
		   public void writeToFile(ArrayList<double[]> A,FileSystem fs,Path p) throws IOException
		   {
			   Path hessbergpath=new Path(hessenbergout+"/hessenberg.martix");
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
			hessenbergout=conf.get("hessenbergout");
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
				System.out.println("X="+getString(X));
				//计算W
				double[] W=getW(X,i);
				System.out.println("W="+getString(W));
				//计算V
				double[] V=getV(X,W)	;
				System.out.println("V="+getString(V));
				//		//计算H ，其中H=1-2P,P=V*Vh/Vh*V
               double VHV=getVHV(V);
               H=getH(V,VHV);
               System.out.println("H=====");
                	for(int m=0;m<H.size();m++)
       		{
       			double[] temp=H.get(m);
       				for(int j=0;j<temp.length;j++)
       				{
       					System.out.print(temp[j]+"  ");
       			}
       			System.out.println();
       	}
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
	   String Q;
	   //QR分解得到的Q写入文件中,Q以列的形式存储
	   public void writeToFile(ArrayList<double[]> A,FileSystem fs,Path p) throws IOException
	   {
		   
		   Path hessbergpath=new Path(Q+"/Q.martix");
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
			 Q=conf.get("Q");
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
				 String Q=conf.get("Q");
				 FSDataInputStream fsdis=fs.open(new Path(Q+"/Q.martix"));
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
				 String eigenout=conf.get("eigenout");
				 fs = p.getFileSystem ( conf) ;
				 FSDataInputStream fsdis=fs.open(new Path(eigenout+"/part-r-00000"));
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
				   System.out.println("CC="+getString(CC));
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
			   System.out.println("eigenlist="+eigenlist);
			   for(int i=0;i<eigenlist.size();i++)
			   {
				   if(eigenlist.get(i)!=currenteigen)
				   {
					   ArrayList<double[]> B=getEA(Alist,eigenlist.get(i));
				//	   System.out.println("templist");
				//	   for(int m=0;m<templist.size();m++)
				//	   {
					//	   double[] p=templist.get(m);
					//	   System.out.println(getString(p));
				//	   }
					//   System.out.println("B");
				//	   for(int m=0;m<B.size();m++)
				//	   {
					//	   double[] p=B.get(m);
					//	   System.out.println(getString(p));
				//	   }
					   templist=getMartixMul(templist,B);
					//   System.out.println("templist");
				//	   for(int m=0;m<templist.size();m++)
					//   {
						//   double[] p=templist.get(m);
					//	   System.out.println(getString(p));
					//   }
				   }
			   }
			   //第0行就是所求的特征向量，进行单位化，模长为1
	          double[] res=getOneVector(templist.get(0));
	          return getString(res);
		   }
		   //将特征值进行从小到大排序
		   public ArrayList<Double> sortasc(ArrayList<Double> list)
		   {
			   for(int i=1;i<list.size();i++)
			   {
				   int j=i;
				   while(j>=1&&list.get(j)<list.get(j-1))
				   {
					   double temp=list.get(j-1);
					   list.set(j-1,list.get(j));
					   list.set(j,temp);
					   j--;
				   }
			   }
			   return list;
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
				 String eigenout=conf.get("eigenout");
				 FSDataInputStream fsdis=fs.open(new Path(eigenout+"/part-r-00000"));
				 String line=fsdis.readLine();
				 while(line!=null)
				 {
					 eigenlist.add(Double.valueOf(line));
					 line=fsdis.readLine();
				 }
				 fsdis.close();
				 eigenlist=sortasc(eigenlist);
				 System.out.println("eigenout="+eigenlist);
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
				 String Q=conf.get("Q");
				 FSDataInputStream fsdis=fs.open(new Path(Q+"/Q.martix"));
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
	   
	   //计算新的矩阵，新的矩阵为原始矩阵，input中的矩阵和特征值大对应的特征向量
	 public static class NewMartixMapper extends Mapper<LongWritable,Text,IntWritable,Text>
	 {
		 Path p;
		 FileSystem fs;
		 int lineindex=0;
		 IntWritable mapkey=new IntWritable();
		 Text mapvalue=new Text();
		 //存储矩阵特征向量
		 ArrayList<double[]> linelist=new ArrayList();
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
					 String eigenvectorout=conf.get("eigenvectorout");
					 FSDataInputStream fsdis=fs.open(new Path(eigenvectorout+"/part-r-00000"));
					 String line=fsdis.readLine();
					 while(line!=null)
					 {
						 double[] q=getArray(line.split("\t")[1]);
						 linelist.add(q);
						 line=fsdis.readLine();
					 }
					 fsdis.close();
					 //k表示用户设定选取几个特征值
					 int k=conf.getInt("choosecount",1);
					 for(int i=0;i<linelist.size()-k;i++)
					 {
						 linelist.remove(i);
					 }
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
			@Override
			protected void map(LongWritable key, Text value,
					org.apache.hadoop.mapreduce.Mapper.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				double[] d=getArray(value.toString());
				mapkey.set(lineindex);
				for(int i=0;i<linelist.size();i++)
				{
					mapvalue.set(i+"_"+getLineMulLine(d,linelist.get(i)));
					context.write(mapkey, mapvalue);
				}
				lineindex++;
			}
	 }
		public static class NewMartixReducer extends Reducer<IntWritable,Text,IntWritable,Text>
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
	 public  void run(long timestamp) throws IOException, ClassNotFoundException, InterruptedException
	   {
		      long atime=System.currentTimeMillis();
		      int loopcount;
			  ParamsManager manager=ParamsManager.getParamsManagerInstance();
			  Configuration conf = new Configuration ( ) ;
			  conf.addResource(new Path("/usr/hadoop/hadoop-1.2.1/conf/core-site.xml"));
			  Path p=new Path(conf.get("fs.default.name"));
			  conf.set("HDFS", conf.get("fs.default.name"));
			  conf.setInt("choosecount", (Integer)manager.getParamsValue(timestamp, "choosecount"));
			  loopcount=(Integer)manager.getParamsValue(timestamp, "loopcount");
			  FileSystem fs = p.getFileSystem ( conf ) ;
			  String parentpath=p.toString();
			//  FileStatus[] fstatus = fs.listStatus ( new Path (p,"/home/novas") ) ;
			  /*
			   * 配置数据存储相关路径
			   */
			  String deletePath = null ;
			  String readPath = null ;//这个参数的含义是用来交替改变读取位置的
			//  readPath=parentpath+"/home/novas/PCA/input";
			  readPath=parentpath+manager.getParamsValue(timestamp, "inputPath");
			//  String out=parentpath+"/home/novas/PCA/out";
			  //转置矩阵输出路径
			  String transmartixout=parentpath+"/"+timestamp+"/home/novas/PCA/transout";
			  //协方差输出路径
			  String covout=parentpath+"/"+timestamp+"/home/novas/PCA/covout";
			  //hessenberg矩阵输出路径
			  String hessenbergout=parentpath+"/"+timestamp+"/home/novas/PCA/hessenbergout";
			  String qrout=parentpath+"/"+timestamp+"/home/novas/PCA/qrout";
			  String eigenout=parentpath+"/"+timestamp+"/home/novas/PCA/eigenout";
			  String eigenvectorout=parentpath+"/"+timestamp+"/home/novas/PCA/eigenvectorout";
			  String newmartixout=parentpath+"/"+timestamp+manager.getParamsValue(timestamp, "outputPath");
			  conf.set("transout",transmartixout);
			  String Q=parentpath+"/"+timestamp+"/home/novas/PCA/Q";
			  conf.set("hessenbergout", hessenbergout);
			  conf.set("Q", Q);
			  conf.set("eigenout", eigenout);
			  conf.set("eigenvectorout", eigenvectorout);
			  fs.delete(new Path(newmartixout));
			//  fs.delete(new Path(out));
			  fs.delete(new Path(transmartixout));
			  fs.delete(new Path(covout));
			  fs.delete(new Path(hessenbergout));
			  fs.delete(new Path(qrout));
			  fs.delete(new Path(eigenout));
			  fs.delete(new Path(eigenvectorout));
	//求矩阵转置的job
			  Job transjob = new Job ( conf ) ;
			  transjob.setJarByClass(pca.class);
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
		 	 covjob.setJarByClass(pca.class);
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
		
		 	 int count=0;
		 	 while(true)
		 	 {
		 		  //qr分解
				  Job qrjob = new Job ( conf ) ;
				  qrjob.setJarByClass(pca.class);
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
				  newhessenbergjob.setJarByClass(pca.class);
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
			     if(count==loopcount)
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
			  eigenjob.setJarByClass(pca.class);
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
		     eigenvectorjob.setJarByClass(pca.class);
		     eigenvectorjob.setMapperClass ( LUMapper.class ) ;
		     eigenvectorjob.setReducerClass ( LUReducer.class ) ;
		     eigenvectorjob.setMapOutputKeyClass(DoubleWritable.class);
		     eigenvectorjob.setMapOutputValueClass(Text.class);
		     eigenvectorjob.setOutputKeyClass ( DoubleWritable.class);
		     eigenvectorjob.setOutputValueClass ( Text.class);
		 	 FileInputFormat.addInputPath ( eigenvectorjob , new Path ( covout ) ) ;
		 	 FileOutputFormat.setOutputPath ( eigenvectorjob ,  new  Path ( eigenvectorout ) ) ;
		     eigenvectorjob.waitForCompletion ( true ) ;
		     //选取特征值，生成新的矩阵,选取特征值的个数设为变量
		     Job newmartixjob = new Job ( conf ) ;
		     newmartixjob.setJarByClass(pca.class);
		     newmartixjob.setMapperClass ( NewMartixMapper.class ) ;
		     newmartixjob.setReducerClass ( NewMartixReducer.class ) ;
		     newmartixjob.setMapOutputKeyClass(IntWritable.class);
		     newmartixjob.setMapOutputValueClass(Text.class);
		     newmartixjob.setOutputKeyClass ( IntWritable.class);
		     newmartixjob.setOutputValueClass ( Text.class);
		 	 FileInputFormat.addInputPath ( newmartixjob , new Path ( readPath ) ) ;
		 	 FileOutputFormat.setOutputPath ( newmartixjob ,  new  Path ( newmartixout ) ) ;
		 	newmartixjob.waitForCompletion ( true ) ;
		     
	   } 
}
