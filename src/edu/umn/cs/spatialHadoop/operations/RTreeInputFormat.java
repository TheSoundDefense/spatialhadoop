package edu.umn.cs.spatialHadoop.operations;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;

import edu.umn.cs.FileRange;
import edu.umn.cs.spatialHadoop.mapReduce.SplitCalculator;


/**
 * Reads a file stored as a list of RTrees
 * @author eldawy
 *
 */
public class RTreeInputFormat extends FileInputFormat<CellInfo, RTree<Shape>> {

	@Override
	public RecordReader<CellInfo, RTree<Shape>> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		// Create record reader
	    reporter.setStatus(split.toString());
		return new RTreeRecordReader(job, (FileSplit)split);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	  
	  // Generate splits for all input paths
	  InputSplit[] splits = super.getSplits(job, numSplits);
	  Vector<FileRange> fileRanges = SplitCalculator.calculateRanges(job);
	  // if processing a heap file, just use all of them
	  if (fileRanges == null)
      return splits;
	  // Early prune file splits that are completely outside query range
    Vector<FileSplit> inputSplits = new Vector<FileSplit>();

    for (InputSplit split : splits) {
      // Check if this input split is in the search space
      if (SplitCalculator.isInputSplitInSearchSpace((FileSplit) split, fileRanges))
        inputSplits.add((FileSplit)split);
    }
	  return inputSplits.toArray(new FileSplit[inputSplits.size()]);
	}

}