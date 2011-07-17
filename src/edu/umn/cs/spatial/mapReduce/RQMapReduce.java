package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;



/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class RQMapReduce {
  public static final String QUERY_SHAPE = "edu.umn.cs.spatial.mapReduce.RQMapReduce.QueryRectangle";
  public static Shape queryShape;

	public static class Map extends MapReduceBase
			implements
			Mapper<LongWritable, TigerShape, LongWritable, TigerShape> {
	  
	  
    	public void map(
    	    LongWritable shapeId,
    			TigerShape tigerShape,
    			OutputCollector<LongWritable, TigerShape> output,
    			Reporter reporter) throws IOException {
    		if (queryShape.isIntersected(tigerShape.shape)) {
    			output.collect(shapeId, tigerShape);
    		}
    	}
    }
	
	/**
	 * Entry point to the file.
	 * Params <query rectangle> <input filenames> <output filename>
	 * query rectangle: in the form x,y,width,height
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(RQMapReduce.class);
      conf.setJobName("BasicRangeQuery");
      
      // Retrieve query rectangle and store it along with the job
      String queryRectangle = args[0];
      // Set the rectangle to be used in map job
      conf.set(QUERY_SHAPE, queryRectangle);
      // Define the subset to be processed of input file
      conf.set(SplitCalculator.QUERY_RANGE, queryRectangle);

      conf.setOutputKeyClass(LongWritable.class);
      conf.setOutputValueClass(TigerShape.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(RQInputFormat.class);
      conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
      conf.setOutputFormat(TextOutputFormat.class);

      // All files except last one are input files
      for (int i = 1; i < args.length - 1; i++)
        RQInputFormat.addInputPath(conf, new Path(args[i]));
      
      // Last argument is the output file
      FileOutputFormat.setOutputPath(conf, new Path(args[args.length - 1]));

      // Start the job
      JobClient.runJob(conf);
    }
}
