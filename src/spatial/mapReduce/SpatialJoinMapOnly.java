package spatial.mapReduce;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import spatial.PairOfRectangles;
import spatial.Rectangle;
import spatial.SpatialAlgorithms;

public class SpatialJoinMapOnly {

    public static class Map extends MapReduceBase implements Mapper<Rectangle, CollectionWritable<Rectangle>, Rectangle, PairOfRectangles> {

    	@Override
    	public void map(
    			Rectangle cell,
    			CollectionWritable<Rectangle> rectangles,
    			OutputCollector<Rectangle, PairOfRectangles> output,
    			Reporter reporter) throws IOException {
    		// Do a spatial join locally on rectangles
			Collection<PairOfRectangles> matches = SpatialAlgorithms
					.spatialJoin(rectangles);
			// Send output to the reducer
			for (PairOfRectangles match : matches) {
				output.collect(cell, match);
			}
    	}
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(SpatialJoinMapOnly.class);
      conf.setJobName("spatialjoin");
      // Set record length to 32 bytes
      conf.set(SpatialRecordReader.RECORD_LENGTH, "32");

      conf.setOutputKeyClass(Rectangle.class);
      conf.setOutputValueClass(PairOfRectangles.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(SpatialInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }
}
