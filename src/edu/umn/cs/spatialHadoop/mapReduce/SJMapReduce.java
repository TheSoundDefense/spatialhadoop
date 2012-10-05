package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.spatial.WriteGridFile;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.TigerShapeWithIndex;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class SJMapReduce {
	public static final Log LOG = LogFactory.getLog(SJMapReduce.class);
	public static final String CELLS_INFO = "edu.umn.cs.spatial.mapReduce.SJMapReduce.CellsInfo";
	public static CellInfo[] cellsInfo;

	/**
	 * Maps each rectangle in the input data set to a grid cell
	 * @author eldawy
	 *
	 */
	public static class Map extends MapReduceBase
	implements
	Mapper<LongWritable, TigerShapeWithIndex, CellInfo, TigerShapeWithIndex> {
	  
		public void map(
				LongWritable id,
				TigerShapeWithIndex shape,
				OutputCollector<CellInfo, TigerShapeWithIndex> output,
				Reporter reporter) throws IOException {

			// output the input rectangle to each grid cell it intersects with
		  for (int cellIndex = 0; cellIndex < cellsInfo.length; cellIndex++) {
		    if (cellsInfo[cellIndex].isIntersected(shape)) {
		      output.collect(cellsInfo[cellIndex], shape);
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
			
			SpatialAlgorithms.SpatialJoin_planeSweep(R, S, output);
		}

	}

	public static GridInfo calculateGridInfo(GridInfo gridInfo, FileSystem outFS, Path[] inputPaths) throws IOException {
	  // Calculate a suitable grid info to use with SJMR
    // 1- If a complete grid info is given (MBR + cell size), use it
    // 2- If no grid info is given and largest file is a grid file and
    //    grid info of largest file covers MBR of all files, use it
    // 3- If no grid info is given or largest file is not a grid or
    //    grid info of largest file does not cover MBR of all files,
    //    generate a grid info using MBR of all files and total files sizes
    long totalSize = 0;
    long sizeOfLargestFile = 0;
    Rectangle mbrOfAllFiles = WriteGridFile.getMBR(outFS, inputPaths[0]);

    GridInfo gridInfoOfLargestFile = null;
    // Find grid info based on files MBRs
    for (Path inputPath : inputPaths) {
      FileStatus fileStatus = outFS.getFileStatus(inputPath);
      totalSize += fileStatus.getLen();
      if (fileStatus.getLen() > sizeOfLargestFile) {
        gridInfoOfLargestFile = fileStatus.getGridInfo();
        sizeOfLargestFile = fileStatus.getLen();
      }
      mbrOfAllFiles = (Rectangle) mbrOfAllFiles.union(WriteGridFile.getMBR(outFS, inputPath));
    }

    LOG.info("SJ gridInfo: " + gridInfo);
    LOG.info("SJ gridInfoOfLargestFile: " + gridInfoOfLargestFile);
    LOG.info("SJ mbrOfAllFiles: "+mbrOfAllFiles);

    // Invalidate any grid info that is not complete or does not contain MBR of all files
    if (gridInfo != null && gridInfo.columns == 0)
      gridInfo = null;
    if (gridInfo != null && !gridInfo.getMBR().contains(mbrOfAllFiles))
      gridInfo = null;
    if (gridInfoOfLargestFile != null && !gridInfoOfLargestFile.getMBR().contains(mbrOfAllFiles))
      gridInfoOfLargestFile = null;
    
    // Initialize grid info to MBR of all files with uninitialized cell
    if (gridInfo == null) {
      gridInfo = gridInfoOfLargestFile;
      LOG.info("SJ gridInfo <- gridInfoOfLargestFile");
    }
    
    if (gridInfo == null) {
      gridInfo = new GridInfo(mbrOfAllFiles.x, mbrOfAllFiles.y, mbrOfAllFiles.width, mbrOfAllFiles.height, 0, 0);
      gridInfo.calculateCellDimensions(totalSize, outFS.getDefaultBlockSize());
      LOG.info("SJ gridInfo calculated using MBR: " + gridInfo);
    }

    LOG.info("Using SJMR with grid: "+gridInfo);

    return gridInfo;
	}

	/**
	 * Entry point to the file.
	 * Params -pack <grid info> <input filenames> <output filename>
	 * grid info: in the form grid:xOrigin,yOrigin,gridWidth,gridHeight,cellWidth,cellHeight. No spaces here please.
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SJMapReduce.class);
		conf.setJobName("Spatial Join");
		
		CommandLineArguments cla = new CommandLineArguments(args);
		GridInfo gridInfo = cla.getGridInfo();
		Path[] inputPaths = cla.getInputPaths();
		Path outputPath = cla.getOutputPath();

		// Get the HDFS file system
		FileSystem inFS = inputPaths[0].getFileSystem(conf);
		FileSystem outFS = outputPath.getFileSystem(conf);
		
		gridInfo = calculateGridInfo(gridInfo, inFS, inputPaths);
		CellInfo[] cellsInfo;
		if (cla.isPack())
		  cellsInfo = WriteGridFile.packInRectangles(inFS, inputPaths, outFS, gridInfo);
		else
		  cellsInfo = gridInfo.getAllCells();

    // Retrieve query rectangle and store it in job info
    conf.set(CELLS_INFO, GridOutputFormat.encodeCells(cellsInfo));

		conf.setOutputKeyClass(CellInfo.class);
		conf.setOutputValueClass(TigerShapeWithIndex.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShapeWithIndex.class.getName());
		conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
		conf.setInputFormat(SJInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// All files except first and last ones are input files
		RQInputFormat.setInputPaths(conf, inputPaths);

		// Last argument is the output file
		FileOutputFormat.setOutputPath(conf, outputPath);

		JobClient.runJob(conf);
	}
}
