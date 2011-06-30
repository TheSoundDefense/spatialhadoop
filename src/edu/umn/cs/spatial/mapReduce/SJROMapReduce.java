package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import edu.umn.edu.spatial.SpatialAlgorithms;


/**
 * This is a reduce only spatial join.
 * As Hadoop doesn't support a reduce only job, a map only job is created
 * but the map functions like a reduce.
 * @author aseldawy
 *
 */
public class SJROMapReduce {
	public static final Log LOG = LogFactory.getLog(SJROMapReduce.class);

	/**
	 * Maps each rectangle in the input data set to a grid cell
	 * @author eldawy
	 *
	 */
	public static class Map extends MapReduceBase
	implements
	Mapper<CollectionWritable<Rectangle>, CollectionWritable<Rectangle>, Rectangle, Rectangle> {

		public void map(
				CollectionWritable<Rectangle> r1,
				CollectionWritable<Rectangle> r2,
				OutputCollector<Rectangle, Rectangle> output,
				Reporter reporter) throws IOException {
		  // Cast arguments to ArrayList to be able to use PlaneSweep Algorithm
		  ArrayListWritable<Rectangle> R = (ArrayListWritable<Rectangle>) r1;
		  ArrayListWritable<Rectangle> S = (ArrayListWritable<Rectangle>) r2;
		  System.out.println("Joining "+R.size()+" with "+S.size());
		  SpatialAlgorithms.SpatialJoin_planeSweep(R, S, output);
		}
	}

	/**
	 * Entry point to the file.
	 * Params <input filename 1> <input filename 2> <output filename>
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SJROMapReduce.class);
		conf.setJobName("Spatial Join Reduce Only");

		conf.setOutputKeyClass(Rectangle.class);
		conf.setOutputValueClass(Rectangle.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(SJROInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// All files except first and last ones are input files
		for (int i = 0; i < args.length - 1; i++)
			RQInputFormat.addInputPath(conf, new Path(args[i]));

		// Last argument is the output file
		Path outputPath = new Path(args[args.length - 1]);
		FileOutputFormat.setOutputPath(conf, outputPath);

		JobClient.runJob(conf);
	}
}
