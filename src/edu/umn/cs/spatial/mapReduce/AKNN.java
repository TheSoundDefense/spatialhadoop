package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import edu.umn.edu.spatial.SpatialAlgorithms;

public class AKNN {
	static final int K = 10;

	public static class Map extends MapReduceBase
			implements
			Mapper<CollectionWritable<Point>, Point, Point, DoubleWritable> {
    	@Override
    	public void map(
    			CollectionWritable<Point> queryPoints,
    			Point s,
    			OutputCollector<Point, DoubleWritable> output,
    			Reporter reporter) throws IOException {
    		
    		for (Point pt : queryPoints) {
    			double distance2 = pt.distanceTo(s);
    			// We use p as the intermediate key because we need only one reducer
    			output.collect(pt, new DoubleWritable(distance2));
    		}
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
      JobConf conf = new JobConf(AKNN.class);
      conf.setJobName("AkNN");
      
      Point[] points = {
    	  new Point(10, 10),
    	  new Point(15, 15),
    	  new Point(20, 15),
      };
      
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
      String histogramFilename = inputFilename+".hist";
      int columns = (int)Math.ceil((gridX2 - gridX1) / gridCellWidth);
      int rows = (int)Math.ceil((gridY2 - gridY1) / gridCellHeight);
      int[][] histogram = SpatialAlgorithms.readHistogram(conf, histogramFilename, columns, rows);

      HashMap<Integer, Vector<Point>> cell2Points = new HashMap<Integer, Vector<Point>>();
      for (Point p : points) {
    	  // Choose cells to read
    	  int[] cells = SpatialAlgorithms.KnnCells(p, histogram, gridX1, gridY1,
    			  gridX2, gridY2, gridCellWidth, gridCellHeight, K);
    	  for (int cell : cells) {
    		  Vector<Point> cellPoints = cell2Points.get(cell);
    		  if (cellPoints == null) {
    			  cellPoints = new Vector<Point>();
    			  cell2Points.put(cell, cellPoints);
    		  }
    		  cellPoints.add(p);
    	  }
      }
      
      // Holds splits of the query points
      String blocks2readR = "o:";
      // Holds splits of the input points (grid)
      String blocks2readS = "s:";
      
      Path tempFilepath = new Path("/aknn.r");
      
      // Get the HDFS file system
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(tempFilepath)) {
    	  // remove the file first
    	  fs.delete(tempFilepath, false);
      }

      // Open an output stream for the file
      FSDataOutputStream out = fs.create(tempFilepath);

      long start = 0;
      
      for (Integer cell : cell2Points.keySet()) {
    	  long length = 0;
    	  
    	  // Write all associated points to a file
    	  for (Point p : cell2Points.get(cell)) {
    		  out.writeInt(p.id);
    		  out.writeFloat(p.x);
    		  out.writeFloat(p.y);
    		  length += 4 + 4*2;
    	  }
    	  blocks2readR += start+"-"+length+",";
    	  start += length;

		  // Append this grid cell to splits
		  blocks2readS += cell+",";
      }
      out.close();
      // Remove trailing comma
      blocks2readR = blocks2readR.substring(0, blocks2readR.length() - 1);
      blocks2readS = blocks2readS.substring(0, blocks2readS.length() - 1);
      // Set the property to SpatialInputFormat
      System.out.println("blocks2readR: "+blocks2readR);
      System.out.println("blocks2readS: "+blocks2readS);
      conf.set(SplitCalculator.BLOCKS2READ+".0", blocks2readR);
      conf.set(SplitCalculator.BLOCKS2READ+".1", blocks2readS);

      conf.setOutputKeyClass(Point.class);
      conf.setOutputValueClass(DoubleWritable.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(AKNNInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // First file is input file
      AKNNInputFormat.addInputPath(conf, tempFilepath);
      for (int i = 0; i < args.length - 1; i++) {
    	  AKNNInputFormat.addInputPath(conf, new Path(args[i]));
      }
      
      // Second file is output file
      FileOutputFormat.setOutputPath(conf, new Path(args[args.length-1]));

      JobClient.runJob(conf);
    }
}
