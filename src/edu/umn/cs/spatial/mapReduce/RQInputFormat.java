package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;
import java.util.Vector;

import javax.annotation.processing.Filer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

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
public class RQInputFormat extends FileInputFormat<Rectangle, Rectangle> {

	@Override
	public RecordReader<Rectangle, Rectangle> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new RQCombineRecordReader((PairOfFileSplits)split, job, reporter);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	  // Generate splits for all input paths
	  InputSplit[] splits = super.getSplits(job, numSplits);
	  Vector<FileRange> fileRanges = SplitCalculator.calculateRanges(job);
	  // Divide the splits into two lists; one for query splits and one for
	  // input splits
    Vector<FileSplit> querySplits = new Vector<FileSplit>();
    Vector<FileSplit> inputSplits = new Vector<FileSplit>();
    Path queryFilePath = listStatus(job)[0].getPath();
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      
      if (fileSplit.getPath().equals(queryFilePath)) {
        querySplits.add((FileSplit)split);
      } else {
        // Check if this input split is in the search space
        if (isInputSplitInSearchSpace((FileSplit) split, fileRanges))
          inputSplits.add((FileSplit)split);
      }
    }
    // Generate a combined file split for each pair of splits
    // i.e. Cartesian product
    int i = 0;
    InputSplit[] combinedSplits = new InputSplit[querySplits.size() * inputSplits.size()];
    for (FileSplit querySplit : querySplits) {
      for (FileSplit inputSplit : inputSplits) {
        combinedSplits[i++] = new PairOfFileSplits(querySplit, inputSplit);
      }
    }
	  return combinedSplits;
	}

	/**
	 * Check if the given file split intersects with any range of the given list
	 * of ranges.
	 * @param split
	 * @param fileRanges
	 * @return <code>true</code> if <code>split</code> intersects with at least
	 * one fileRange in the given list.
	 */
  private boolean isInputSplitInSearchSpace(FileSplit split, Vector<FileRange> fileRanges) {
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
