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

import edu.umn.edu.spatial.PairOfRectangles;
import edu.umn.edu.spatial.Rectangle;
import edu.umn.edu.spatial.SpatialAlgorithms;

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
public class SJROInputFormat extends FileInputFormat<CollectionWritable<Rectangle>, CollectionWritable<Rectangle>> {

	@Override
	public RecordReader<CollectionWritable<Rectangle>, CollectionWritable<Rectangle>> getRecordReader(InputSplit split,
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
	  Vector<FileSplit>[] splitLists = new Vector[inputFiles.length];
	  
	  // Holds a rectangle or more for each file split
	  Vector<Rectangle>[] rectangles = new Vector[inputFiles.length];
	  
	  for (int i = 0; i < inputFiles.length; i++) {
	    // Initialize an empty list to hold all splits for this file
	    splitLists[i] = new Vector<FileSplit>();
	    rectangles[i] = new Vector<Rectangle>();

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
	        // Append it to the list
	        int splitIndex = splitLists[i].size();
	        splitLists[i].add(fileSplit);
	        
	        // Create a rectangle for each grid cell it spans
	        for (BlockLocation blockLocation : blockLocations) {
	          if (blockLocation.getOffset() < fileSplit.getStart() + fileSplit.getLength() &&
	              fileSplit.getStart() < blockLocation.getOffset() + blockLocation.getLength()) {
	            // Part of this split overlaps this file block
	            Rectangle rect = new Rectangle(splitIndex, (int)blockLocation.getCellInfo().x,
	                (int)blockLocation.getCellInfo().y,
	                (int)(blockLocation.getCellInfo().x+gridInfo.cellWidth),
	                (int)(blockLocation.getCellInfo().y+gridInfo.cellHeight));
	            rectangles[i].add(rect);
	          }
	        }
	      }
	    }
	  }
	  
	  // Now we need to join the two rectangles list to find all pairs of overlapping splits
	  final Vector<PairOfRectangles> pairs = new Vector<PairOfRectangles>();
	  OutputCollector<Rectangle, Rectangle> output = new OutputCollector<Rectangle, Rectangle>() {
      @Override
      public void collect(Rectangle r, Rectangle s) throws IOException {
        System.out.println("Joining "+r+" with "+ s);
        pairs.add(new PairOfRectangles(r, s));
      }
    };
    
    // Do the spatial join between grid cells
    SpatialAlgorithms.SpatialJoin_planeSweep(rectangles[0], rectangles[1], output);

    // Generate a combined file split for each pair of splits
    int i = 0;
    Set<String> takenPairs = new HashSet<String>();
    InputSplit[] combinedSplits = new InputSplit[pairs.size()];
    for (PairOfRectangles por : pairs) {
      // Check that this pair was not taken before
      String thisPair = (int)por.r1.id+","+(int)por.r2.id;
      if (!takenPairs.contains(thisPair)) {
        takenPairs.add(thisPair);
        // Each rectangle here represents a file split.
        // Rectangle.id represents the index of the split in the array
        FileSplit fileSplit1 = splitLists[0].get((int)por.r1.id);
        FileSplit fileSplit2 = splitLists[1].get((int)por.r2.id);

        combinedSplits[i++] = new PairOfFileSplits(fileSplit1, fileSplit2);
      }
    }
	  return combinedSplits;
	}
}
