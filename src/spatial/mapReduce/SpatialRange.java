package spatial.mapReduce;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
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

public class SpatialRange {

	public static class Map extends MapReduceBase
			implements
			Mapper<Rectangle, CollectionWritable<Rectangle>, Rectangle, Rectangle> {

		Rectangle range = new Rectangle(1,1,1,100,100);
    	@Override
    	public void map(
    			Rectangle cell,
    			CollectionWritable<Rectangle> rects,
    			OutputCollector<Rectangle, Rectangle> output,
    			Reporter reporter) throws IOException {
    		
    		Collection<Rectangle> matches = SpatialAlgorithms.Range(range, rects, -1);
    			for (Rectangle match : matches) {
    				output.collect(cell, match);
    			}    		
    	}
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(SpatialRange.class);
      conf.setJobName("spatialrange");
      // Set record length to 32 bytes
      conf.set(SpatialRecordReader.RECORD_LENGTH, "32");

      conf.setOutputKeyClass(Rectangle.class);
      conf.setOutputValueClass(Rectangle.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(SpatialInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      for (int i = 0; i < args.length - 1; i++) {
    	  SpatialInputFormat.addInputPath(conf, new Path(args[i]));
      }
      FileOutputFormat.setOutputPath(conf, new Path(args[args.length-1]));

      JobClient.runJob(conf);
    }
}
