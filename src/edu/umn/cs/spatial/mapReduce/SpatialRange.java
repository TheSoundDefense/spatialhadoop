package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
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


public class SpatialRange {
	/**
	 * The range I'm going to query.
	 */
	static Rectangle queryRange;

	/**
	 * The mapper class
	 * @author aseldawy
	 *
	 */
	public static class Map extends MapReduceBase
			implements
			Mapper<Rectangle, Rectangle, Rectangle, Rectangle> {

    	@Override
    	public void map(
    			Rectangle cell,
    			Rectangle r,
    			OutputCollector<Rectangle, Rectangle> output,
    			Reporter reporter) throws IOException {
    		int condition = -1;
    		
    		if (condition == -1 || condition == r.type) {
				if (queryRange.intersects(r))
					output.collect(cell, r);
    		}
    	}
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(SpatialRange.class);
      conf.setJobName("spatialrange");

      // Initialize query rectangle
      queryRange = new Rectangle(1, 1, 1, 100, 100);
      
      // Get input file information (Grid information)
      Path inputFile = new Path(args[0]);
      FileStatus fileStatus = inputFile.getFileSystem(conf).getFileStatus(inputFile);
      
      System.out.println("grid rectangle: "+fileStatus.getGridX1()+", "+fileStatus.getGridY1()+
    		  ", "+fileStatus.getGridX2()+", "+fileStatus.getGridY2());
      // Determine which blocks are needed
      int x1 = (int)Math.floor((queryRange.x1 - fileStatus.getGridX1()) / fileStatus.getGridCellWidth());
      int y1 = (int)Math.floor((queryRange.y1 - fileStatus.getGridY1()) / fileStatus.getGridCellHeight());
      int x2 = (int)Math.floor((queryRange.x2 - fileStatus.getGridX1()) / fileStatus.getGridCellWidth());
      int y2 = (int)Math.floor((queryRange.y2 - fileStatus.getGridY1()) / fileStatus.getGridCellHeight());
      
      // Set the property to SpatialInputFormat
      String blocks2read = "r:"+x1+","+y1+","+x2+","+y2;
      System.out.println("blocks2read: "+blocks2read);
      conf.set(SpatialRangeInputFormat.BLOCKS2READ+".0", blocks2read);
      
      // Set configuration for this MapRedue job
      conf.setOutputKeyClass(Rectangle.class);
      conf.setOutputValueClass(Rectangle.class);

      conf.setMapperClass(Map.class);

      conf.setInputFormat(SpatialRangeInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // First argument is input file
	  SpatialRangeInputFormat.addInputPath(conf, inputFile);

	  // Last argument is output file
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      // Start MapReduce job
      JobClient.runJob(conf);
    }
}
