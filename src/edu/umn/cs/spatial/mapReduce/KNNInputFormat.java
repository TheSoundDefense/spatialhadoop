package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;

import edu.umn.edu.spatial.Point;
import edu.umn.edu.spatial.Rectangle;



/**
 * Reads and parses a file that contains records of type Rectangle.
 * Records are assumed to be fixed size and of the format
 * <id>,<left>,<top>,<right>,<bottom>
 * When a record of all zeros is encountered, it is assumed to be the end of file.
 * This means, no more records are processed after a zero-record.
 * Records are read one be one.
 * @author aseldawy
 *
 */
public class KNNInputFormat extends FileInputFormat<Rectangle, Point> {

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

	@Override
	public RecordReader<Rectangle, Point> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		@SuppressWarnings("unchecked")
		Class<RecordReader<Rectangle, Rectangle>> klass =
			(Class<RecordReader<Rectangle, Rectangle>>) RectangleRecordReader.class
				.asSubclass(RecordReader.class);

		return new PointRecordReader((FileSplit)split, job, reporter);
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) 
	throws IOException {
	    FileStatus[] files = listStatus(job);
	    
	    // Save the number of input files for metrics/loadgen
	    job.setLong(NUM_INPUT_FILES, files.length);
	    long totalSize = 0;                           // compute total size
	    for (FileStatus file: files) {                // check we have valid files
	      if (file.isDirectory()) {
	        throw new IOException("Not a file: "+ file.getPath());
	      }
	      totalSize += file.getLen();
	    }

	    // generate splits
	    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
	    NetworkTopology clusterMap = new NetworkTopology();
	    int i = 0;
	    for (FileStatus file: files) {
	      Path path = file.getPath();
	      FileSystem fs = path.getFileSystem(job);
	      long length = file.getLen();
	      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
	      if ((length != 0) && isSplitable(fs, path)) {
		    	Vector<Long> starts = new Vector<Long>();
		    	Vector<Long> lengths = new Vector<Long>();
		    	String blocks2readStr = job.get(SpatialJoinInputFormat.BLOCKS2READ+'.'+i++, "a");
		    	SplitCalculator.calculateSplits(job, file, starts, lengths, blocks2readStr);

				for (int splitNum = 0; splitNum < starts.size(); splitNum++) {
					long splitStart = starts.elementAt(splitNum);
					long splitSize = lengths.elementAt(splitNum);
					String[] splitHosts = getSplitHosts(blkLocations,
							splitStart, splitSize, clusterMap);
					splits.add(makeSplit(path, splitStart,
							splitSize, splitHosts));
				}
	      } else { 
	        //Create empty hosts array for zero length files
	        splits.add(makeSplit(path, 0, length, new String[0]));
	      }
	    }
	    LOG.debug("Total # of splits: " + splits.size());
	    return splits.toArray(new FileSplit[splits.size()]);
	}
	/*	Path[] paths = FileUtil.stat2Paths(listStatus(job));

		// Initialize arrays for splits to be created
		Vector<Long>[] starts = new Vector[paths.length];
		Vector<Long>[] lengths = new Vector[paths.length];

		for (int i = 0; i < paths.length; i++) {
			// Find block size and total size of this file
			FileStatus fileStatus = paths[i].getFileSystem(job).getFileStatus(paths[i]);
			
			starts[i] = new Vector<Long>();
			lengths[i] = new Vector<Long>();
			

			calculateSplits(
		}


		// Be sure that all files have the same number of blocks to read
		for (int i = 1; i < paths.length; i++) {
			if (starts[i].size() != starts[i-1].size())
				throw new RuntimeException("Cannot split two files to different split sizes");
		}

		numSplits = starts[0].size() * paths.length;
		FileSplit[] splits = new FileSplit[numSplits];
		NetworkTopology clusterMap = new NetworkTopology();
		
		int splitNum = 0;
		
		for (int pathNum = 0; pathNum < paths.length; pathNum++) {
			FileStatus file = listStatus(job)[pathNum];
			Path path = paths[pathNum];
			FileSystem fs = path.getFileSystem(job);
			for (int i = 0; i < starts[0].size(); i++) {
				long start = starts[pathNum].elementAt(i);
				long length = lengths[pathNum].elementAt(i);
				// Initialize locations like in org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file, start, length);
				String[] splitHosts = getSplitHosts(blkLocations,0,length,clusterMap);
				splits[splitNum++] = makeSplit(path, 0, length, splitHosts);
			}
		}
		
		System.out.println("Created "+ numSplits + " input splits");
		return splits;
	}
*/
}
