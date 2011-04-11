package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.umn.edu.spatial.Rectangle;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class RQMapReduce {

	public static class Map extends MapReduceBase
			implements
			Mapper<Rectangle, Rectangle, Rectangle, Rectangle> {
    	public void map(
    	    Rectangle queryRectangle,
    			Rectangle inputRectangle,
    			OutputCollector<Rectangle, Rectangle> output,
    			Reporter reporter) throws IOException {

    		if (queryRectangle.intersects(inputRectangle)) {
    			output.collect(queryRectangle, inputRectangle);
    		}
    	}
    }
	
	/**
	 * Entry point to the file.
	 * Params <query rectangle> <input filenames> <output filename>
	 * query rectangle: in the form x1,y1,x2,y2
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(RQMapReduce.class);
      conf.setJobName("BasicRangeQuery");
      
      // Retrieve query rectangle and store it to an HDFS file
      Rectangle queryRectangle = new Rectangle();
      String[] parts = args[0].split(",");
      
      queryRectangle.x1 = Float.parseFloat(parts[0]);
      queryRectangle.y1 = Float.parseFloat(parts[1]);
      queryRectangle.x2 = Float.parseFloat(parts[2]);
      queryRectangle.y2 = Float.parseFloat(parts[3]);
      
      // Get the HDFS file system
      FileSystem fs = FileSystem.get(conf);
      Path queryFilepath = new Path("/range_query");
      if (fs.exists(queryFilepath)) {
        // remove the file first
        fs.delete(queryFilepath, false);
      }
      // Open an output stream for the file
      FSDataOutputStream out = fs.create(queryFilepath);
      PrintStream ps = new PrintStream(out);
      ps.print(0+","+queryRectangle.x1 +","+ queryRectangle.y1 +","+
          queryRectangle.x2+","+queryRectangle.y2);
      ps.close();

      // add this query file as the first input path to the job
      RQInputFormat.addInputPath(conf, queryFilepath);
      
      conf.setOutputKeyClass(Rectangle.class);
      conf.setOutputValueClass(Rectangle.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(RQInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // All files except last one are input files
      for (int i = 1; i < args.length - 1; i++)
        RQInputFormat.addInputPath(conf, new Path(args[i]));
      
      // Last argument is the output file
      FileOutputFormat.setOutputPath(conf, new Path(args[args.length - 1]));

      JobClient.runJob(conf);
    }
}
