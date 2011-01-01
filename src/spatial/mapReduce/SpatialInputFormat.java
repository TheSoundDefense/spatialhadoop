package spatial.mapReduce;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import spatial.Rectangle;


/**
 * Reads and parses a file that contains records of type Rectangle.
 * Records are assumed to be fixed size and of the format
 * <id>,<left>,<top>,<right>,<bottom>
 * When a record of all zeros is encountered, it is assumed to be the end of file.
 * This means, no more records are processed after a zero-record.
 * All records are read in one chunk and a single record is emitted with a key
 * of a bounding rectangle and a value of all rectangles read.
 * @author aseldawy
 *
 */
public class SpatialInputFormat extends FileInputFormat<Rectangle, Collection<Rectangle>> {

	@Override
	public RecordReader<Rectangle, Collection<Rectangle>> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
	    return new SpatialRecordReader(job, (FileSplit) split);
	}

	protected long computeSplitSize(long goalSize, long minSize,
			long blockSize) {
		return blockSize;
	}

}
