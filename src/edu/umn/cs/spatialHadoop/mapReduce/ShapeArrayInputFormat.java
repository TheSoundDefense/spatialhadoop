package edu.umn.cs.spatialHadoop.mapReduce;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;

/**
 * Reads a file stored as a list of RTrees
 * @author eldawy
 *
 */
public class ShapeArrayInputFormat extends SpatialInputFormat<CellInfo, ArrayWritable> {

	@Override
	public RecordReader<CellInfo, ArrayWritable> getRecordReader(InputSplit split,
	    JobConf job, Reporter reporter) throws IOException {
	  // Create record reader
	  reporter.setStatus(split.toString());
		return new ShapeArrayRecordReader(job, (FileSplit)split);
	}
}