package edu.umn.cs.spatialHadoop.mapReduce;

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
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatialHadoop.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.operations.PairOfFileSplits;

/**
 * An input format that reads a pair of files simultaneously and returns
 * a key for one of them and the value as a pair of values. 
 * @author eldawy
 *
 */
public class PairInputFormat<K, V> extends FileInputFormat<K, Pair<V>> {

  @Override
  public RecordReader<K, Pair<V>> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new PairRecordReader<K, V>((PairOfFileSplits)split, job, reporter);
  }
  
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // Get a list of all input files. There should be exactly two files.
    FileStatus[] inputFiles = listStatus(job);

    // Generate splits for all input paths
    final InputSplit[] splits = super.getSplits(job, numSplits);
    
    // Holds a rectangle or more for each file split.
    // Most probably one rectangle for each split. The shape ID points to
    // the split index in the list and the rectangle points to its boundaries.
    @SuppressWarnings("unchecked")
    Vector<TigerShape>[] spatialSplits = new Vector[inputFiles.length];

    // Holds a list of heap splits for each file
    @SuppressWarnings("unchecked")
    Vector<Integer>[] heapSplits = new Vector[inputFiles.length];

    for (int file_i = 0; file_i < inputFiles.length; file_i++) {
      spatialSplits[file_i] = new Vector<TigerShape>();
      // Extract all blocks for this file
      BlockLocation[] blockLocations = inputFiles[file_i].getPath()
          .getFileSystem(job)
          .getFileBlockLocations(inputFiles[file_i], 0, inputFiles[file_i].getLen());

      // Filter all splits to this file
      for (int splitIndex = 0; splitIndex < splits.length; splitIndex++) {
        FileSplit fileSplit = (FileSplit) splits[splitIndex];
        if (fileSplit.getPath().equals(inputFiles[file_i].getPath())) {
          // Create a rectangle for each grid cell it spans
          for (BlockLocation blockLocation : blockLocations) {
            if (blockLocation.getOffset() < fileSplit.getStart()
                + fileSplit.getLength()
                && fileSplit.getStart() < blockLocation.getOffset()
                + blockLocation.getLength()) {
              if (blockLocation.getCellInfo() == null) {
                heapSplits[file_i].add(splitIndex);
              } else {
                // Part of this split overlaps this file block
                TigerShape rect = new TigerShape(blockLocation.getCellInfo(),
                    splitIndex);
                spatialSplits[file_i].add(rect);
              }
            }
          }
        }
      }
    }
    
    // This list will hold the pair of overlapping file splits
    final Vector<InputSplit> combinedSplits = new Vector<InputSplit>();

    // Now we need to join the two spatial splits list to find all pairs
    // of overlapping splits
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
          FileSplit fileSplit1 = (FileSplit)splits[(int)r.id];
          FileSplit fileSplit2 = (FileSplit)splits[(int)s.id];

          combinedSplits.add(new PairOfFileSplits(fileSplit1, fileSplit2));
        }
      }
    };

    // Do the spatial join between grid cells
    SpatialAlgorithms.SpatialJoin_planeSweep(spatialSplits[0], spatialSplits[1], output);

    // For a heap split, we need to process it with every other split in
    // the other file
    for (int file_i = 0; file_i < heapSplits.length; file_i++) {
      for (int split_i : heapSplits[file_i]) {
        for (int file_j = file_i + 1; file_j < heapSplits.length; file_j++) {
          // First, join with other heap splits
          for (int split_j : heapSplits[file_j]) {
            combinedSplits.add(new PairOfFileSplits((FileSplit)splits[split_i],
                (FileSplit)splits[split_j]));
          }
          // Second, join with spatial splits
          for (TigerShape split_j : spatialSplits[file_j]) {
            combinedSplits.add(new PairOfFileSplits((FileSplit)splits[split_i],
                (FileSplit)splits[(int)split_j.id]));
          }
        }
      }
    }

    return combinedSplits.toArray(new InputSplit[combinedSplits.size()]);
  }

}
