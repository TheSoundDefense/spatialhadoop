package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.spatial.Rectangle;

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
public class SJInputFormat extends FileInputFormat<GridInfo, Rectangle> {

	@Override
	public RecordReader<GridInfo, Rectangle> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new SJCombineRecordReader((PairOfFileSplits)split, job, reporter);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	  // Generate splits for all input paths
	  InputSplit[] splits = super.getSplits(job, numSplits);

	  // Divide the splits into two lists; one for query splits and one for
	  // input splits
    Vector<FileSplit> gridInfoSplits = new Vector<FileSplit>();
    Vector<FileSplit> inputSplits = new Vector<FileSplit>();
    Path gridInfoFilePath = listStatus(job)[0].getPath();
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      
      if (fileSplit.getPath().equals(gridInfoFilePath)) {
        gridInfoSplits.add((FileSplit)split);
      } else {
        inputSplits.add((FileSplit)split);
      }
    }
    // Generate a combined file split for each pair of splits
    // i.e. Cartesian product
    int i = 0;
    InputSplit[] combinedSplits = new InputSplit[gridInfoSplits.size() * inputSplits.size()];
    for (FileSplit querySplit : gridInfoSplits) {
      for (FileSplit inputSplit : inputSplits) {
        combinedSplits[i++] = new PairOfFileSplits(querySplit, inputSplit);
      }
    }
	  return combinedSplits;
	}
}
