package edu.umn.cs.spatialHadoop.operations;

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
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.ArrayListWritable;
import edu.umn.cs.CollectionWritable;
import edu.umn.cs.spatialHadoop.SpatialAlgorithms;


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
	Mapper<CollectionWritable<TigerShape>, CollectionWritable<TigerShape>, TigerShape, TigerShape> {

		public void map(
				CollectionWritable<TigerShape> r1,
				CollectionWritable<TigerShape> r2,
				OutputCollector<TigerShape, TigerShape> output,
				Reporter reporter) throws IOException {
		  // Cast arguments to ArrayList to be able to use PlaneSweep Algorithm
		  ArrayListWritable<TigerShape> R = (ArrayListWritable<TigerShape>) r1;
		  ArrayListWritable<TigerShape> S = (ArrayListWritable<TigerShape>) r2;
		  System.out.println("Joining "+R.size()+" with "+S.size());
		  SpatialAlgorithms.SpatialJoin_planeSweep(R, S, output);
		}
	}

	public static void SJRO(JobConf conf, Path[] inputFiles, Path outputPath) throws IOException {
    conf.setJobName("Spatial Join Reduce Only");

    conf.setOutputKeyClass(TigerShape.class);
    conf.setOutputValueClass(TigerShape.class);

    conf.setMapperClass(Map.class);

    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
    conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShape.class.getName());
    conf.setInputFormat(CollectionPairInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    // All files except first and last ones are input files
    CollectionPairInputFormat.setInputPaths(conf, inputFiles);

    // Last argument is the output file
    FileOutputFormat.setOutputPath(conf, outputPath);

    JobClient.runJob(conf);
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

		Path[] inputFiles = new Path[args.length - 1];
    for (int i = 0; i < args.length - 1; i++)
      inputFiles[i] = new Path(args[i]);
    
    Path outputPath = new Path(args[args.length - 1]);
    
    SJRO(conf, inputFiles, outputPath);
}
}
