package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.spatial.Rectangle;
import edu.umn.cs.spatial.SpatialAlgorithms;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class SJMapReduce {
	public static final Log LOG = LogFactory.getLog(SJMapReduce.class);

	/**
	 * Maps each rectangle in the input data set to a grid cell
	 * @author eldawy
	 *
	 */
	public static class Map extends MapReduceBase
	implements
	Mapper<GridInfo, Rectangle, Rectangle, Rectangle> {
		private static Hashtable<Integer, Rectangle> cellRectangles = new Hashtable<Integer, Rectangle>();

		private static Rectangle getCellRectangle(GridInfo gridInfo, int cellCol, int cellRow) {
			int cellNumber = cellRow * 10000 + cellCol;
			Rectangle cellRectangle = cellRectangles.get(cellNumber);
			if (cellRectangle == null) {
				int cellX = (int) (cellCol * gridInfo.cellWidth + gridInfo.xOrigin);
				int cellY = (int) (cellRow * gridInfo.cellHeight + gridInfo.yOrigin);
				cellRectangle = new Rectangle(0, cellX, cellY, cellX + (int)gridInfo.cellWidth, cellY +(int)gridInfo.cellHeight);
				cellRectangles.put(cellNumber, cellRectangle);
			}
			return cellRectangle;
		}

		public void map(
				GridInfo gridInfo,
				Rectangle rectangle,
				OutputCollector<Rectangle, Rectangle> output,
				Reporter reporter) throws IOException {

			// output the input rectangle to each grid cell it intersects with
			int cellCol1 = (int) ((rectangle.getX1() - gridInfo.xOrigin) / gridInfo.cellWidth);
			int cellRow1 = (int) ((rectangle.getY1() - gridInfo.yOrigin) / gridInfo.cellHeight);
			int cellCol2 = (int) ((rectangle.getX2() - gridInfo.xOrigin) / gridInfo.cellWidth);
			int cellRow2 = (int) ((rectangle.getY2() - gridInfo.yOrigin) / gridInfo.cellHeight);

			for (int cellCol = cellCol1; cellCol <= cellCol2; cellCol++) {
				for (int cellRow = cellRow1; cellRow <= cellRow2; cellRow++) {
					Rectangle cellRectangle = getCellRectangle(gridInfo, cellCol, cellRow);
					output.collect(cellRectangle, rectangle);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Rectangle, Rectangle, Rectangle, Rectangle> {
		@Override
		public void reduce(Rectangle key, Iterator<Rectangle> values,
				OutputCollector<Rectangle, Rectangle> output, Reporter reporter)
		throws IOException {
			List<Rectangle> rects[] = new List[10];
			// do a spatial join over rectangles in the values set
			// and output each joined pair to the output
			while (values.hasNext()) {
				Rectangle rect = (Rectangle) values.next().clone();
				if (rects[rect.type] == null)
					rects[rect.type] = new ArrayList<Rectangle>();
				rects[rect.type].add(rect);
			}

			List<Rectangle> R = null, S = null;
			for (List<Rectangle> rectanglesList : rects) {
				if (rectanglesList != null) {
					if (R == null)
						R = rectanglesList;
					else
						S = rectanglesList;
				}
			}
			// In case they're empty
      if (R == null)
        R = new ArrayList<Rectangle>();
      if (S == null)
        S = new ArrayList<Rectangle>();
			
			SpatialAlgorithms.SpatialJoin_planeSweep(R, S, output);
		}

	}


	/**
	 * Entry point to the file.
	 * Params <grid info> <input filenames> <output filename>
	 * grid info: in the form xOrigin,yOrigin,gridWidth,gridHeight,cellWidth,cellHeight. No spaces here please.
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SJMapReduce.class);
		conf.setJobName("Spatial Join");

		// Retrieve query rectangle and store it to an HDFS file
		GridInfo gridInfo = new GridInfo();
		String[] parts = args[0].split(",");

		gridInfo.xOrigin = Long.parseLong(parts[0]);
		gridInfo.yOrigin = Long.parseLong(parts[1]);
		gridInfo.gridWidth = Long.parseLong(parts[2]);
		gridInfo.gridHeight = Long.parseLong(parts[3]);
		gridInfo.cellWidth = Long.parseLong(parts[4]);
		gridInfo.cellHeight = Long.parseLong(parts[5]);

		// Get the HDFS file system
		FileSystem fs = FileSystem.get(conf);
		
		// If files are grid files, use the gridinfo of the largest files
		// instead of the one passed
		long maxSize = 0;
		for (int i = 1; i < args.length - 1; i++) {
		  FileStatus fileStatus = fs.getFileStatus(new Path(args[i]));
		  if (fileStatus.getLen() > maxSize && fileStatus.getGridInfo() != null) {
		    gridInfo = fileStatus.getGridInfo();
		    maxSize = fileStatus.getLen();
		  }
		}

		// Write grid info to a temporary file
		Path gridInfoFilepath = new Path("/sj_grid_info");
		FSDataOutputStream out = fs.create(gridInfoFilepath, true);
		PrintStream ps = new PrintStream(out);
		ps.println(args[0]);
		ps.close();

		// add this query file as the first input path to the job
		SJInputFormat.addInputPath(conf, gridInfoFilepath);

		conf.setOutputKeyClass(Rectangle.class);
		conf.setOutputValueClass(Rectangle.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(SJInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// All files except first and last ones are input files
		Path[] inputPaths = new Path[args.length - 2];
		for (int i = 1; i < args.length - 1; i++)
			RQInputFormat.addInputPath(conf, inputPaths[i-1] = new Path(args[i]));

		// Last argument is the output file
		Path outputPath = new Path(args[args.length - 1]);
		FileOutputFormat.setOutputPath(conf, outputPath);

		JobClient.runJob(conf);
	}
}
