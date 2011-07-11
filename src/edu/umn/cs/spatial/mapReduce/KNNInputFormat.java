package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.Rectangle;

import edu.umn.cs.spatial.TigerShape;

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
public class KNNInputFormat extends FileInputFormat<LongWritable, TigerShape> {

	@Override
	public RecordReader<LongWritable, TigerShape> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		return new TigerPointRecordReader(job, (FileSplit)split);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // TODO move this part to another code that gets processed once for each mapper
    // Set QueryRange in the mapper class
    String queryRangeStr = job.get(RQMapReduce.QUERY_SHAPE);
    String[] parts = queryRangeStr.split(",");
    RQMapReduce.queryShape = new Rectangle(Long.parseLong(parts[0]),
        Long.parseLong(parts[1]), Long.parseLong(parts[2]),
        Long.parseLong(parts[3]));    
    
    // Generate splits for all input paths
    InputSplit[] splits = super.getSplits(job, numSplits);
    Vector<FileRange> fileRanges = SplitCalculator.calculateRanges(job);
    // Divide the splits into two lists; one for query splits and one for
    // input splits
    Vector<FileSplit> inputSplits = new Vector<FileSplit>();

    for (InputSplit split : splits) {
      // Check if this input split is in the search space
      if (SplitCalculator.isInputSplitInSearchSpace((FileSplit) split, fileRanges))
        inputSplits.add((FileSplit)split);
    }
    return inputSplits.toArray(new FileSplit[inputSplits.size()]);
  }
	
}
