package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.umn.edu.spatial.Point;
import edu.umn.edu.spatial.Rectangle;
import edu.umn.edu.spatial.SpatialAlgorithms;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class RQMapReduce {
	/** Query rectangle */
	private static Rectangle queryRectangle;

	public static class Map extends MapReduceBase
			implements
			Mapper<IntWritable, Rectangle, IntWritable, Rectangle> {
    	public void map(
    			IntWritable rectId,
    			Rectangle rect,
    			OutputCollector<IntWritable, Rectangle> output,
    			Reporter reporter) throws IOException {

    		if (queryRectangle.intersects(rect)) {
    			output.collect(rectId, rect);
    		}
    	}
    }
	
	/**
	 * Entry point to the file.
	 * Params <input filename> <output filename> <query rectangle>
	 * input filename: A path to an input file in HDFS
	 * output filename: A path to an output file in HDFS
	 * query rectangle: in the form x1,y1,x2,y2
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(RQMapReduce.class);
      conf.setJobName("BasicRangeQuery");
      queryRectangle = new Rectangle();
      String[] parts = args[2].split(",");
      
      queryRectangle.x1 = Float.parseFloat(parts[0]);
      queryRectangle.y1 = Float.parseFloat(parts[1]);
      queryRectangle.x2 = Float.parseFloat(parts[2]);
      queryRectangle.y2 = Float.parseFloat(parts[3]);
      
      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Rectangle.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(RQInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // First file is input file
	  RQInputFormat.addInputPath(conf, new Path(args[0]));
      
      // Second file is output file
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}
