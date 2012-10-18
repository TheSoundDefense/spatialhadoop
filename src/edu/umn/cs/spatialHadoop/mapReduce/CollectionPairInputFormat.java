package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.CollectionWritable;

/**
 * Reads two splits at the same time from two different files. Each file contains a list
 * of TigerShapes. The associated RecordReader reads a list of TigerShapes from each file
 * and generates one key/value pair where the key is the list of shapes from file 1
 * and value is the shapes from file 2.
 * @author aseldawy
 *
 */
public class CollectionPairInputFormat extends FileInputFormat<CollectionWritable<TigerShape>, CollectionWritable<TigerShape>> {
  public static final Log LOG = LogFactory.getLog(CollectionPairInputFormat.class);

	@Override
	public RecordReader<CollectionWritable<TigerShape>, CollectionWritable<TigerShape>> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new CollectionPairRecordReader((PairOfFileSplits)split, job, reporter);
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

      // Find grid info for this file
      GridInfo gridInfo = inputFiles[i].getGridInfo();

      if (gridInfo != null)
        rectangles[i] = new Vector<TigerShape>();
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

          if (gridInfo != null) {
            // Create a rectangle for each grid cell it spans
            for (BlockLocation blockLocation : blockLocations) {
              if (blockLocation.getOffset() < fileSplit.getStart()
                  + fileSplit.getLength()
                  && fileSplit.getStart() < blockLocation.getOffset()
                  + blockLocation.getLength()) {
                // Part of this split overlaps this file block
                TigerShape rect = new TigerShape(blockLocation.getCellInfo(),
                    splitIndex);
                rectangles[i].add(rect);
              }
            }
          }
        }
      }
    }
	  
    final Vector<InputSplit> combinedSplits = new Vector<InputSplit>();

    if (rectangles[0] != null && rectangles[1] != null) {
      // Now we need to join the two rectangles list to find all pairs of overlapping splits
      final Set<String> takenPairs = new HashSet<String>();
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
    } else {
      LOG.info("Cartesian product of splits from both files");
      // Join every pair
      for (FileSplit fileSplit1 : splitLists[0]) {
        for (FileSplit fileSplit2 : splitLists[1]) {
          combinedSplits.add(new PairOfFileSplits(fileSplit1, fileSplit2));
        }        
      }
    }

	  return combinedSplits.toArray(new InputSplit[combinedSplits.size()]);
	}
}
