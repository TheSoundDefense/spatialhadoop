package org.spatialjoin;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SpatialJoin
{
    private static final double LLX = -126;
    private static final double LLY = 32;
    private static final double URX = -114;
    private static final double URY = 44;
    private static final double XSPLIT = -120;
    private static final double YSPLIT = 38;

    public static class Map extends MapReduceBase implements 
						      Mapper<LongWritable, 
						      Text, 
						      IntWritable, 
						      Text>
    {
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output,
			Reporter reporter) throws IOException
	{
	    Text MBR = new Text();
	    String line = value.toString();
	    StringTokenizer st = new StringTokenizer(line);
	    while(st.hasMoreTokens())
	    {
		double llx = Double.parseDouble(st.nextToken());
		double lly = Double.parseDouble(st.nextToken());
		double urx = Double.parseDouble(st.nextToken());
		double ury = Double.parseDouble(st.nextToken());

		int cell = partition(llx,lly);
		MBR.set(""+llx+" "+lly+" "+urx+" "+ury);
		output.collect(new IntWritable(cell),MBR);
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
	    Vector<MBR> mbrs = new Vector<MBR>();
	    Vector<MBRPair> left = new Vector<MBRPair>();
	    Vector<MBRPair> right = new Vector<MBRPair>();
	    int count = 0;
	    while(values.hasNext())
	    {
		String tmp = values.next().toString();
		StringTokenizer st = new StringTokenizer(tmp);
		double llx = Double.parseDouble(st.nextToken());
		double lly = Double.parseDouble(st.nextToken());
		double urx = Double.parseDouble(st.nextToken());
		double ury = Double.parseDouble(st.nextToken());

		mbrs.add(new MBR(llx,lly,urx,ury,count));
		left.add(new MBRPair(llx,count));
		right.add(new MBRPair(urx,count));
		count++;
	    }

	    Collections.sort(left);
	    Collections.sort(right);

	    Vector<Integer> active = new Vector<Integer>();

	    int lindex = 0;
	    int rindex = 0;
	    MBRPair lpair = left.get(lindex++);
	    MBRPair rpair = right.get(rindex++);

	    for(;;)
	    {
		if(lpair.x < rpair.x)
		{
		    MBR r = mbrs.get(lpair.oid);
		    for(int i = 0; i < active.size(); i++)
		    {
			MBR tmp = mbrs.get(active.get(i).intValue());
			if(MBRintersect(r,tmp))
			    output.collect(key,new Text(""+r.oid+" "+tmp.oid));
		    }
		    active.add(new Integer(lpair.oid));
		    if(lindex == left.size()-1) break;
		    lpair = left.get(lindex++);
		}
		else
		{
		    active.remove(new Integer(rpair.oid));
		    rpair = right.get(rindex++);
		}
	    }
	}
    }

    public static void main(String[] args) throws IOException
    {
	JobConf conf = new JobConf(SpatialJoin.class);
	conf.setJobName("spatialjoin");

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
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

    private static class MBR
    {
	public final double llx;
	public final double lly;
	public final double urx;
	public final double ury;
	public final int oid;

	public MBR(double llx, double lly, double urx, double ury, int oid)
	{
	    this.llx = llx;
	    this.lly = lly;
	    this.urx = urx;
	    this.ury = ury;
	    this.oid = oid;
	}
    }

    private static class MBRPair implements Comparable<MBRPair>
    {
	public final double x;
	public final int oid;
	
	public MBRPair(double x, int oid)
	{
	    this.x = x;
	    this.oid = oid;
	}

	public int compareTo(MBRPair m)
	{
	    double tmp = x-m.x;
	    if(tmp == 0) return 0;
	    else if(tmp < 0) return -1;
	    else return 1;
	}
    }

    private static boolean MBRintersect(MBR a, MBR b)
    {
	if(a.llx >= b.urx) return false;
	if(a.ury <= b.lly) return false;
	if(a.urx <= b.llx) return false;
	if(a.lly >= b.ury) return false;
	return true;
    }
}
