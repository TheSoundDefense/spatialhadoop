package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatial.SpatialAlgorithms;
import edu.umn.cs.spatial.TigerShapeWithIndex;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class SJMapReduce {
	public static final Log LOG = LogFactory.getLog(SJMapReduce.class);
	public static final String GRID_INFO = "edu.umn.cs.spatial.mapReduce.SJMapReduce.GridInfo";
	public static GridInfo gridInfo;

	/**
	 * Maps each rectangle in the input data set to a grid cell
	 * @author eldawy
	 *
	 */
	public static class Map extends MapReduceBase
	implements
	Mapper<LongWritable, TigerShapeWithIndex, CellInfo, TigerShapeWithIndex> {
	  
		private static Hashtable<Integer, CellInfo> cellRectangles = new Hashtable<Integer, CellInfo>();

		private static CellInfo getCellRectangle(GridInfo gridInfo, int cellCol, int cellRow) {
			int cellNumber = (int)Point.mortonOrder(cellCol, cellRow);
			CellInfo cellInfo = cellRectangles.get(cellNumber);
			if (cellInfo == null) {
				long cellX = cellCol * gridInfo.cellWidth + gridInfo.xOrigin;
				long cellY = cellRow * gridInfo.cellHeight + gridInfo.yOrigin;
				cellInfo = new CellInfo(cellX, cellY, gridInfo.cellWidth, gridInfo.cellHeight);
				cellRectangles.put(cellNumber, cellInfo);
			}
			return cellInfo;
		}

		public void map(
				LongWritable id,
				TigerShapeWithIndex shape,
				OutputCollector<CellInfo, TigerShapeWithIndex> output,
				Reporter reporter) throws IOException {

			// output the input rectangle to each grid cell it intersects with
		  Rectangle rectangle = shape.getMBR();
			int cellCol1 = (int) ((rectangle.getX1() - gridInfo.xOrigin) / gridInfo.cellWidth);
			int cellRow1 = (int) ((rectangle.getY1() - gridInfo.yOrigin) / gridInfo.cellHeight);
			int cellCol2 = (int) ((rectangle.getX2() - gridInfo.xOrigin) / gridInfo.cellWidth);
			int cellRow2 = (int) ((rectangle.getY2() - gridInfo.yOrigin) / gridInfo.cellHeight);

			for (int cellCol = cellCol1; cellCol <= cellCol2; cellCol++) {
				for (int cellRow = cellRow1; cellRow <= cellRow2; cellRow++) {
					CellInfo cellInfo = getCellRectangle(gridInfo, cellCol, cellRow);
					output.collect(cellInfo, shape);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<CellInfo, TigerShapeWithIndex, TigerShape, TigerShape> {
		@Override
		public void reduce(CellInfo cellInfo, Iterator<TigerShapeWithIndex> values,
				OutputCollector<TigerShape, TigerShape> output, Reporter reporter)
		throws IOException {
			List<TigerShapeWithIndex> shapes[] = new List[2];
			// do a spatial join over rectangles in the values set
			// and output each joined pair to the output
			while (values.hasNext()) {
				TigerShapeWithIndex shape = (TigerShapeWithIndex) values.next().clone();
				if (shapes[shape.index] == null)
					shapes[shape.index] = new ArrayList<TigerShapeWithIndex>();
				shapes[shape.index].add(shape);
			}

			List<TigerShapeWithIndex> R = null, S = null;
			for (List<TigerShapeWithIndex> shapesList : shapes) {
				if (shapesList != null) {
					if (R == null)
						R = shapesList;
					else
						S = shapesList;
				}
			}
			// In case they're empty
      if (R == null)
        R = new ArrayList<TigerShapeWithIndex>();
      if (S == null)
        S = new ArrayList<TigerShapeWithIndex>();
			
      System.out.println("Joining "+R.size()+" with "+S.size());
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
		gridInfo.readFromString(args[0]);

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

    // Retrieve query rectangle and store it in job info
    conf.set(GRID_INFO, gridInfo.writeToString());

		conf.setOutputKeyClass(CellInfo.class);
		conf.setOutputValueClass(TigerShapeWithIndex.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShapeWithIndex.class.getName());
		conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
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
