package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.umn.edu.spatial.Rectangle;


public class KNN {
	static final int K = 10;
	static Rectangle p;

	public static class Map extends MapReduceBase
			implements
			Mapper<Rectangle, Rectangle, Rectangle, DoubleWritable> {
    	@Override
    	public void map(
    			Rectangle cell,
    			Rectangle s,
    			OutputCollector<Rectangle, DoubleWritable> output,
    			Reporter reporter) throws IOException {
    		
    		double distance2 = p.distanceTo(s);
    		// We use p as the intermediate key because we need only one reducer
    		output.collect(p, new DoubleWritable(distance2));
    	}
    }
	
	public static class Reduce extends MapReduceBase
		implements
		Reducer<Rectangle, DoubleWritable, Rectangle, DoubleWritable> {

		@Override
		public void reduce(Rectangle key, Iterator<DoubleWritable> values,
				OutputCollector<Rectangle, DoubleWritable> output,
				Reporter reporter) throws IOException {
			// TODO select top k
			// XXX for now, just select any k items
			Vector<DoubleWritable> topk = new Vector<DoubleWritable>();
			while (values.hasNext()) {
				DoubleWritable next = values.next();
				if (topk.size() < K)
					topk.add(next);
			}
			for (DoubleWritable value : topk) {
				output.collect(key, value);
			}
		}
		
	}

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(KNN.class);
      conf.setJobName(K+"-NN");

      conf.setOutputKeyClass(Rectangle.class);
      conf.setOutputValueClass(DoubleWritable.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reducer.class);

      conf.setInputFormat(SpatialRangeInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      for (int i = 0; i < args.length - 1; i++) {
    	  SpatialJoinInputFormat.addInputPath(conf, new Path(args[i]));
      }
      FileOutputFormat.setOutputPath(conf, new Path(args[args.length-1]));

      JobClient.runJob(conf);
    }
}
