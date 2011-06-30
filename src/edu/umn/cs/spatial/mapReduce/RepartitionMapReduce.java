package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;

import edu.umn.edu.spatial.Rectangle;


/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author aseldawy
 *
 */
public class RepartitionMapReduce {

	public static class Map extends MapReduceBase
			implements
			Mapper<LongWritable, Rectangle, IntWritable, Rectangle> {
	  private static final IntWritable dummy = new IntWritable(1);
    	public void map(
    	    LongWritable id,
    			Rectangle rect,
    			OutputCollector<IntWritable, Rectangle> output,
    			Reporter reporter) throws IOException {

    	  output.collect(dummy, rect);
    	}
    }
	
	/**
	 * Entry point to the file.
	 * Params <gridInfo> <input filenames> <output filename>
	 * gridInfo in the format <x1,y1,w,h,cw,ch>
	 * input filenames: Input file in HDFS
	 * output filename: Outputfile in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(RepartitionMapReduce.class);
      Path inputFile = new Path(args[1]);
      Path outputPath = new Path(args[2]);
      conf.setJobName("Repartition");
      
      // Save gridInfo in job configuration
      conf.set(RectOutputFormat.OUTPUT_GRID, args[0]);
      
      // Override output file
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(outputPath)) {
        // remove the file first
        fs.delete(outputPath, false);
      }

      // add this query file as the first input path to the job
      RepartitionInputFormat.addInputPath(conf, inputFile);
      
      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(Rectangle.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(RepartitionInputFormat.class);
      conf.setOutputFormat(RectOutputFormat.class);

      // Last argument is the output file
      FileOutputFormat.setOutputPath(conf,outputPath);

      JobClient.runJob(conf);
      
      // Rename output file to required name
      // Check that results are correct
      FileStatus[] resultFiles = fs.listStatus(outputPath);
      for (FileStatus resultFile : resultFiles) {
        if (resultFile.getLen() > 0) {
          Path temp = new Path("/tempppp");
          fs.rename(resultFile.getPath(), temp);
          
          fs.delete(outputPath, true);
          
          fs.rename(temp, outputPath);
        }
      }
    }
}
