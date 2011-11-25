package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.util.StringUtils;

import edu.umn.cs.FileRange;
import edu.umn.cs.spatialHadoop.PointWithK;
import edu.umn.cs.spatialHadoop.TigerShapeWithDistance;

public class SplitCalculator {
  public static final Log LOG = LogFactory.getLog(SplitCalculator.class);
	/**
	 * Property name for records to read.
	 * This property can be used to choose the blocks to read in one of four ways:
	 * 0- All: All file blocks are read. Just set the value to the letter 'a'.
	 * 1- Range: Define two grid cells as corners of a rectangle.
	 *   All blocks for grid cells in this range are added.
	 *   Set the value to 'r:top,left,right,bottom'.
	 * 2- Select: Define a list of block numbers. Only these blocks are read.
	 *   Set the value to 's:b1,b2,...,bn'
	 * 3- Offset: Define a list of byte ranges [from, to].
	 *   A split is created for each byte range.
	 *   This is the most general one.
	 *   Set the value to 'o:start-length,start-length,...,start-length'
	 */
	public static final String BLOCKS2READ =
		"mapreduce.input.byterecordreader.record.blocks2read";

	public static final String QUERY_RANGE = 
		"spatial_hadoop.query_range";

	public static final String QUERY_POINT_DISTANCE = 
		"spatial_hadoop.query_point";

	public static Vector<FileRange> calculateRanges(JobConf job) throws IOException {
		if (job.get(QUERY_RANGE) != null)
			return RQCalculateRanges(job);
		if (job.get(QUERY_POINT_DISTANCE) != null)
			return KNNCalculateRanges(job);
		
		LOG.info("Processing the whole file");
		return null;
	}

	/**
	 * Calculate ranges in each input file that need to be processed.
	 * 
	 * For range query:
	 * We include all blocks with grid boundaries intersecting with the query range.
	 * If the file is non-spatial, all the file is added directly.
	 * @param conf
	 * @return
	 * @throws IOException 
	 */
	public static Vector<FileRange> RQCalculateRanges(JobConf conf) throws IOException {
		// Find query range. We assume there is only one query range for the job

		Rectangle queryRange = new Rectangle();
		queryRange.readFromString(conf.get(QUERY_RANGE));
		LOG.info("Restricting blocks according to the range: "+queryRange);

		Vector<FileRange> ranges = new Vector<FileRange>();
		// Retrieve a list of all input files
		String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.
				FileInputFormat.INPUT_DIR, "");
		String [] list = StringUtils.split(dirs);
		Path[] inputPaths = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			inputPaths[i] = new Path(StringUtils.unEscapeString(list[i]));
		}

		// Retrieve list of blocks in each input path
		for (Path path : inputPaths) {
			FileSystem fs = path.getFileSystem(conf);
			Long fileLength = fs.getFileStatus(path).getLen();
			// Retrieve grid info for this file
			GridInfo gridInfo = fs.getFileStatus(path).getGridInfo();
			// Check each block
			BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0, fileLength);
			for (BlockLocation blockLocation : blockLocations) {
				CellInfo cellInfo = blockLocation.getCellInfo();
				// 2- Check if block holds a grid cell in query range
				if (cellInfo == null) {
				  LOG.info("Matched a cell of a heap file");
          ranges.add(new FileRange(path, blockLocation.getOffset(), blockLocation.getLength()));
				} else if (cellInfo.isIntersected(queryRange)) {
				  LOG.info("Matched cell: " + cellInfo +" with query: "+ queryRange);
					// Add this block
					ranges.add(new FileRange(path, blockLocation.getOffset(), blockLocation.getLength()));
				}
			}
			// TODO merge consecutive ranges in the same file
		}

		return ranges;
	}

	/**
	 * Return all blocks within the range of a query point
	 * @param fileSystem
	 * @param filePath
	 * @param queryPoint
	 * @param matchedBlocks
	 * @throws IOException
	 */
  public static void KNNBlocksInRange(FileSystem fileSystem,
      Path filePath, TigerShapeWithDistance queryPoint, Vector<BlockLocation> matchedBlocks) throws IOException {
    double requiredDistance = queryPoint.distance;
    LOG.info("Matching blocks withing a distance of "+requiredDistance+" of the point: "+queryPoint);
	  long fileLength = fileSystem.getFileStatus(filePath).getLen();
	  BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(filePath, 0, fileLength);
	  for (BlockLocation blockLocation : blockLocations) {
	    CellInfo blockMBR = blockLocation.getCellInfo();
	    if (blockMBR == null) {
	      LOG.info("Heap block matched with distance");
	      matchedBlocks.add(blockLocation);
	    } else {
	      double minDistanceToBlock = blockMBR.getMinDistanceTo(queryPoint);
	      LOG.info("Minimum distance for block "+blockMBR+" is "+minDistanceToBlock);
	      if (minDistanceToBlock <= requiredDistance) {
	        LOG.info("Block "+blockLocation.getCellInfo()+" matched with distance: "+minDistanceToBlock);
	        matchedBlocks.add(blockLocation);
	      }
	    }
	  }
	}
	
	/**
	 * Calculate ranges in each input file that need to be processed.
	 * 
	 * For range query:
	 * We include all blocks with grid boundaries intersecting with the query range.
	 * If the file is non-spatial, all the file is added directly.
	 * @param conf
	 * @return
	 * @throws IOException 
	 */
	public static Vector<FileRange> KNNCalculateRanges(JobConf conf) throws IOException {
		// Find query range. We assume there is only one query range for the job
		String queryPointString = conf.get(QUERY_POINT_DISTANCE);
		
		String[] splits = queryPointString.split(",");
    long x = Long.parseLong(splits[0]);
    long y = Long.parseLong(splits[1]);
    long distance = Long.parseLong(splits[2]);
		TigerShapeWithDistance queryPoint = new TigerShapeWithDistance(0, new Point(x,y), distance);
    LOG.info("Restricting blocks according to the point: "+queryPoint);

		Vector<FileRange> ranges = new Vector<FileRange>();
		// Retrieve a list of all input files
		String dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.
				FileInputFormat.INPUT_DIR, "");
		String [] list = StringUtils.split(dirs);
		Path[] inputPaths = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			inputPaths[i] = new Path(StringUtils.unEscapeString(list[i]));
		}

		// Retrieve list of blocks in each input path
		for (Path path : inputPaths) {
			FileSystem fs = path.getFileSystem(conf);
			Vector<BlockLocation> blocksToBeProcessed = new Vector<BlockLocation>();
			KNNBlocksInRange(fs, path, queryPoint, blocksToBeProcessed);

			for (BlockLocation blockLocation : blocksToBeProcessed) {
			  LOG.info("Going to process the block at: "+blockLocation.getCellInfo());
			  ranges.add(new FileRange(path, blockLocation.getOffset(), blockLocation.getLength()));
			}
			// TODO merge consecutive ranges in the same file
		}

		return ranges;
	}

	 /**
   * Check if the given file split intersects with any range of the given list
   * of ranges.
   * @param split
   * @param fileRanges
   * @return <code>true</code> if <code>split</code> intersects with at least
   * one fileRange in the given list.
   */
  public static boolean isInputSplitInSearchSpace(FileSplit split, Vector<FileRange> fileRanges) {
    for (FileRange fileRange : fileRanges) {
      if (fileRange.file.equals(split.getPath()) &&
          !((fileRange.start >= split.getStart() + split.getLength()) ||
              split.getStart() >= fileRange.start + fileRange.length)) {
        return true;
      }
    }
    return false;
  }
}
