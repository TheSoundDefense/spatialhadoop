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

public class NN {

	public static class Map extends MapReduceBase
			implements
			Mapper<Rectangle, CollectionWritable<CollectionWritable<Rectangle>>, Rectangle, PairOfRectangles> {
       static final int K = 10;
    	@Override
    	public void map(
    			Rectangle cell,
    			CollectionWritable<CollectionWritable<Rectangle>> rectanglesLists,
    			OutputCollector<Rectangle, PairOfRectangles> output,
    			Reporter reporter) throws IOException {
    		
    		Collection<PairOfRectangles> matches = SpatialAlgorithms.NN(rectanglesLists,K);
    			for (PairOfRectangles match : matches) {
    				output.collect(cell, match);
    			}
    		
    	}
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(NN.class);
      conf.setJobName(Map.K+"-NN");
      // Set record length to 32 bytes
      conf.set(SpatialRecordReader.RECORD_LENGTH, "32");

      conf.setOutputKeyClass(Rectangle.class);
      conf.setOutputValueClass(PairOfRectangles.class);

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
