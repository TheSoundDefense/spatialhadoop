package org.knn;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class KNN
{
    private static final double LLX = -126;
    private static final double LLY = 32;
    private static final double URX = -114;
    private static final double URY = 44;
    private static final double XSPLIT = -120;
    private static final double YSPLIT = 38;

    private static final double QX = -116;
    private static final double QY = 41;
    private static final int K = 10;

    public static void main(String[] args) throws IOException
    {
	JobConf conf = new JobConf(KNN.class);
	conf.setJobName("knn");

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf,new Path(args[0]));
	FileOutputFormat.setOutputPath(conf,new Path(args[1]));

	JobClient.runJob(conf);
    }

    public static class Map extends MapReduceBase implements
						      Mapper<LongWritable,
						      Text,
						      IntWritable,
						      Text>
    {
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable,Text> output,
			Reporter reporter) throws IOException
	{
	    Text point = new Text();
	    String line = value.toString();
	    StringTokenizer st = new StringTokenizer(line);
	    while(st.hasMoreTokens())
	    {
		double x = Double.parseDouble(st.nextToken());
		double y = Double.parseDouble(st.nextToken());
		
		int cell = partition(x,y);
		point.set(""+x+" "+y);
		output.collect(new IntWritable(cell),point);
	    }
	}	
    }

    public static class Reduce extends MapReduceBase implements
							 Reducer<IntWritable,
							 Text,
							 IntWritable,
							 Text>
    {
	public void reduce(IntWritable key, Iterator<Text> values,
			   OutputCollector<IntWritable, Text> output,
			   Reporter reporter) throws IOException
	{
	    Vector<Double> xvec = new Vector<Double>();
	    Vector<Double> yvec = new Vector<Double>();
	    Vector<KNNDist> distvec = new Vector<KNNDist>();
	    int count = 0;

	    while(values.hasNext())
	    {
		String tmp = values.next().toString();
		StringTokenizer st = new StringTokenizer(tmp);

		double x = Double.parseDouble(st.nextToken());
		double y = Double.parseDouble(st.nextToken());

		xvec.add(new Double(x));
		yvec.add(new Double(y));

		double dist = Math.sqrt(Math.pow(QX-x,2)+Math.pow(QY-y,2));
		distvec.add(new KNNDist(dist,count));
		count++;
	    }

	    Collections.sort(distvec);

	    for(int i = 0; i < K; i++)
	    {
		KNNDist kd = distvec.get(i);
		output.collect(key,new Text(""+i+" "+xvec.get(kd.index)+" "+yvec.get(kd.index)+" "+kd.dist));
	    }
	}
    }
  
    private static int partition(double x, double y)
    {
	if(x < XSPLIT)
	{
	    if(y < YSPLIT) return 2;
	    else return 0;
	}
	else
	{
	    if(y < YSPLIT) return 3;
	    else return 1;
	}
    }

    private static class KNNDist implements Comparable<KNNDist>
    {
	public final double dist;
	public final int index;

	public KNNDist(double dist, int index)
	{
	    this.dist = dist;
	    this.index = index;
	}

	public int compareTo(KNNDist d)
	{
	    double tmp = dist-d.dist;
	    if(tmp == 0) return 0;
	    else if(tmp < 0) return -1;
	    else return 1;
	}
    }
}
