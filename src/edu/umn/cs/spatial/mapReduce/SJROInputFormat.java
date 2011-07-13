package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.CollectionWritable;
import edu.umn.cs.spatial.SpatialAlgorithms;
import edu.umn.cs.spatial.TigerShape;

/**
 * Reads and parses a file that contains records of type Rectangle.
 * Records are assumed to be fixed size and of the format
 * <id>,<left>,<top>,<right>,<bottom>
 * When a record of all zeros is encountered, it is assumed to be the end of file.
 * This means, no more records are processed after a zero-record.
 * Records are read one be one.
 * Input is a pair of files and splits generated are {@link PairOfFileSplits}.
 * A pair of splits is generated for each two splits coming from different files and have
 * overlapping rectangles.
 * @author aseldawy
 *
 */
public class SJROInputFormat extends FileInputFormat<CollectionWritable<TigerShape>, CollectionWritable<TigerShape>> {

	@Override
	public RecordReader<CollectionWritable<TigerShape>, CollectionWritable<TigerShape>> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new SJROCombineRecordReader((PairOfFileSplits)split, job, reporter);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	  // Get a list of all input files. There should be exactly two files.
	  FileStatus[] inputFiles = listStatus(job);

	  // Generate splits for all input paths
	  InputSplit[] splits = super.getSplits(job, numSplits);
	  
	  // Form two lists, one for splits from each file
	  final Vector<FileSplit>[] splitLists = new Vector[inputFiles.length];
	  
	  // Holds a rectangle or more for each file split. Most probably one rectangle for each split
	  Vector<TigerShape>[] rectangles = new Vector[inputFiles.length];

	  for (int i = 0; i < inputFiles.length; i++) {
	    // Initialize an empty list to hold all splits for this file
	    splitLists[i] = new Vector<FileSplit>();
	    rectangles[i] = new Vector<TigerShape>();

	    // Find grid info for this file
	    GridInfo gridInfo = inputFiles[i].getGridInfo();

	    // Extract all blocks for this file
      BlockLocation[] blockLocations = inputFiles[i].getPath()
          .getFileSystem(job)
          .getFileBlockLocations(inputFiles[i], 0, inputFiles[i].getLen());

	    // Filter all splits to this file
	    for (InputSplit split : splits) {
	      FileSplit fileSplit = (FileSplit) split;
	      if (fileSplit.getPath().equals(inputFiles[i].getPath())) {
	        // This split is for current file
	        // Append it to the list of splits for this file
	        int splitIndex = splitLists[i].size();
	        splitLists[i].add(fileSplit);
	        
	        // Create a rectangle for each grid cell it spans
	        for (BlockLocation blockLocation : blockLocations) {
	          if (blockLocation.getOffset() < fileSplit.getStart() + fileSplit.getLength() &&
	              fileSplit.getStart() < blockLocation.getOffset() + blockLocation.getLength()) {
	            // Part of this split overlaps this file block
	            TigerShape rect = new TigerShape(blockLocation.getCellInfo(), splitIndex);
	            rectangles[i].add(rect);
	          }
	        }
	      }
	    }
	  }
	  
	  // Now we need to join the two rectangles list to find all pairs of overlapping splits
	  final Set<String> takenPairs = new HashSet<String>();
	  final Vector<InputSplit> combinedSplits = new Vector<InputSplit>();
	  OutputCollector<TigerShape, TigerShape> output = new OutputCollector<TigerShape, TigerShape>() {
	    @Override
	    public void collect(TigerShape r, TigerShape s) throws IOException {
	      // Generate a combined file split for each pair of splits
	      // Check that this pair was not taken before
	      String thisPair = r.id+","+r.id;
	      if (!takenPairs.contains(thisPair)) {
	        takenPairs.add(thisPair);
	        // Each rectangle here represents a file split.
	        // Rectangle.id represents the index of the split in the array
	        FileSplit fileSplit1 = splitLists[0].get((int)r.id);
	        FileSplit fileSplit2 = splitLists[1].get((int)s.id);

	        combinedSplits.add(new PairOfFileSplits(fileSplit1, fileSplit2));
	      }
	    }
	  };
    
    // Do the spatial join between grid cells
    SpatialAlgorithms.SpatialJoin_planeSweep(rectangles[0], rectangles[1], output);

	  return combinedSplits.toArray(new InputSplit[combinedSplits.size()]);
	}
}
