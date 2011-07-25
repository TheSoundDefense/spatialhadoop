package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.Vector;

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

public class SplitCalculator {
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

	public static final String QUERY_POINT = 
		"spatial_hadoop.query_point";

	public static Vector<FileRange> calculateRanges(JobConf job) throws IOException {
		if (job.get(QUERY_RANGE) != null)
			return RQCalculateRanges(job);
		if (job.get(QUERY_POINT) != null)
			return KNNCalculateRanges(job);
		else
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
			if (gridInfo == null) {
				// Add all the file without checking
				ranges.add(new FileRange(path, 0, fileLength));
			} else {
				// Check each block
				BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0, fileLength);
				for (BlockLocation blockLocation : blockLocations) {
					CellInfo cellInfo = blockLocation.getCellInfo();
					System.out.println("Checking " + cellInfo +" with "+ queryRange);
					// 2- Check if block holds a grid cell in query range
					if (cellInfo.isIntersected(queryRange)) {
					  System.out.println("Matched");
						// Add this block
						ranges.add(new FileRange(path, blockLocation.getOffset(), blockLocation.getLength()));
					}
				}
			}
			// TODO merge consecutive ranges in the same file
		}

		return ranges;
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
		String queryPointString = conf.get(QUERY_POINT);

		String[] splits = queryPointString.split(",");
		int x = Integer.parseInt(splits[0]);
		int y = Integer.parseInt(splits[1]);
		//int k = Integer.parseInt(splits[2]);
		Point queryPoint = new Point(x, y);

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
			if (gridInfo == null) {
				// Add all the file without checking
				ranges.add(new FileRange(path, 0, fileLength));
			} else {
				// Check each block
				BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0, fileLength);
				for (BlockLocation blockLocation : blockLocations) {
					CellInfo cellInfo = blockLocation.getCellInfo();
					// 2- Check if block holds a grid cell in query range
					if (cellInfo.contains(queryPoint)) {
						// Add this block
						ranges.add(new FileRange(path, blockLocation.getOffset(), blockLocation.getLength()));
					}
				}
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
