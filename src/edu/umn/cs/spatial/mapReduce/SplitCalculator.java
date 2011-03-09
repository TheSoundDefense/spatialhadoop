package edu.umn.cs.spatial.mapReduce;

import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;

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

	public static void calculateSplits(JobConf job, FileStatus fileStatus,
			Vector<Long> starts, Vector<Long> lengths, String blocks2readStr) {
		// Get limited records to read (if required)
		String[] parts = blocks2readStr.split(":", 2);
		System.out.println("parts[0]: " + parts[0]);
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
			int columns = (int) ((fileStatus.getGridX2() - fileStatus
					.getGridX1()) / fileStatus.getGridCellWidth());
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

}
