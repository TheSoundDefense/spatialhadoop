package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.util.StringUtils;

import edu.umn.edu.spatial.Rectangle;

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

	public static void calculateSplits(JobConf job, FileStatus fileStatus,
			Vector<Long> starts, Vector<Long> lengths, String blocks2readStr) {
		// Get limited records to read (if required)
		String[] parts = blocks2readStr.split(":", 2);

		if (parts[0].equals("a")) {
			// Add all file blocks
			long start = 0;
			while (start < fileStatus.getLen()) {
				long length = Math.min(fileStatus.getBlockSize(),
						fileStatus.getLen() - start);
				starts.add(start);
				lengths.add(length);
				start += length;
			}
		} else if (parts[0].equals("r")) {
			// Rectangular range
			parts = parts[1].split(",");
			int x1 = Integer.parseInt(parts[0]);
			int y1 = Integer.parseInt(parts[1]);
			int x2 = Integer.parseInt(parts[2]);
			int y2 = Integer.parseInt(parts[3]);
			int columns = (int) (fileStatus.getGridInfo().gridWidth / fileStatus.getGridInfo().cellWidth);
			for (int x = x1; x <= x2; x++) {
				for (int y = y1; y <= y2; y++) {
					long start = (columns * y + x) * fileStatus.getBlockSize();
					long length = Math.min(fileStatus.getBlockSize(),
							fileStatus.getLen() - start);
					starts.add(start);
					lengths.add(length);
				}
			}
		} else if (parts[0].equals("s")) {
			// Select blocks
			parts = parts[1].split(",");
			for (String part : parts) {
				int blockNum = Integer.parseInt(part);
				long start = blockNum * fileStatus.getBlockSize();
				long length = Math.min(fileStatus.getBlockSize(),
						fileStatus.getLen() - start);
				starts.add(start);
				lengths.add(length);
			}
		} else if (parts[0].equals("o")) {
			// Blocks by offset
			parts = parts[1].split(",");
			for (String part : parts) {
				String[] startLength = part.split("-");
				long start = Long.parseLong(startLength[0]);
				long length = Long.parseLong(startLength[1]);
				starts.add(start);
				lengths.add(length);
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
	public static Vector<FileRange> calculateRanges(JobConf conf) throws IOException {
	  // Find query range. We assume there is only one query range for the job
	  String queryRangeString = conf.get(QUERY_RANGE);
	  if (queryRangeString == null) {
	    throw new RuntimeException("Query range not set for the job");
	  }
	  String[] splits = queryRangeString.split(",");
    double x1 = Double.parseDouble(splits[0]);
    double y1 = Double.parseDouble(splits[1]);
    double x2 = Double.parseDouble(splits[2]);
    double y2 = Double.parseDouble(splits[3]);
    Rectangle queryRange = new Rectangle(0, (float)x1, (float)y1, (float)x2, (float)y2);
	  
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
          Rectangle blockRange = new Rectangle(0, (float)cellInfo.x, (float)cellInfo.y,
              (float)cellInfo.x+(float)gridInfo.cellWidth, (float) cellInfo.y+(float)gridInfo.cellHeight);
          if (blockRange.intersects(queryRange)) {
            // Add this block
            ranges.add(new FileRange(path, blockLocation.getOffset(), blockLocation.getLength()));
          }
	      }
	    }
	    // TODO merge consecutive ranges in the same file
	  }

	  return ranges;
	}

}
