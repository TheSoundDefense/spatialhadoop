package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
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

import edu.umn.edu.spatial.Point;
import edu.umn.edu.spatial.Rectangle;
import edu.umn.edu.spatial.SpatialAlgorithms;


public class KNN {
	static final int K = 10;
	static Point p;

	public static class Map extends MapReduceBase
			implements
			Mapper<Rectangle, Point, Point, DoubleWritable> {
    	@Override
    	public void map(
    			Rectangle gridCell,
    			Point s,
    			OutputCollector<Point, DoubleWritable> output,
    			Reporter reporter) throws IOException {
    		
    		double distance2 = p.distanceTo(s);
    		// We use p as the intermediate key because we need only one reducer
    		output.collect(p, new DoubleWritable(distance2));
    	}
    }
	
	public static class Reduce extends MapReduceBase
		implements
		Reducer<Point, DoubleWritable, Point, DoubleWritable> {

		@Override
		public void reduce(Point key, Iterator<DoubleWritable> values,
				OutputCollector<Point, DoubleWritable> output,
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
      conf.setJobName("kNN");
      p = new Point(1, 100, 100);
      
      // Get input file information (Grid information)
      Path inputFile = new Path(args[0]);
      FileStatus fileStatus = inputFile.getFileSystem(conf).getFileStatus(inputFile);
      
      // Determine which blocks are needed
      double gridX1 = fileStatus.getGridX1();
      double gridY1 = fileStatus.getGridY1();
      double gridX2 = fileStatus.getGridX2();
      double gridY2 = fileStatus.getGridY2();
      double gridCellWidth = fileStatus.getGridCellWidth();
      double gridCellHeight = fileStatus.getGridCellWidth();

      // Read histogram
      String inputFilename = args[0];
      String histogramFilename = args[0]+".hist";
      int columns = (int)Math.ceil((gridX2 - gridX1) / gridCellWidth);
      int rows = (int)Math.ceil((gridY2 - gridY1) / gridCellHeight);
      int[][] histogram = SpatialAlgorithms.readHistogram(conf, histogramFilename, columns, rows);

      // Choose cells to read
      int[] cells = SpatialAlgorithms.KnnCells(p, histogram, gridX1, gridY1,
    		  gridX2, gridY2, gridCellWidth, gridCellHeight, K);
      
      // Set the property to SpatialInputFormat
      String blocks2read = "s:"+cells[0]+","+cells[1]+","+cells[2];
      System.out.println("blocks2read: "+blocks2read);
      conf.set(SplitCalculator.BLOCKS2READ+".0", blocks2read);

      conf.setOutputKeyClass(Point.class);
      conf.setOutputValueClass(DoubleWritable.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(KNNInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // First file is input file
      for (int i = 0; i < args.length - 1; i++) {
    	  SpatialJoinInputFormat.addInputPath(conf, new Path(args[i]));
      }
      
      // Second file is output file
      FileOutputFormat.setOutputPath(conf, new Path(args[args.length-1]));

      JobClient.runJob(conf);
    }
}
