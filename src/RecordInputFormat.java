import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class RecordInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

	@Override
	public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
	    return new ByteRecordReader(job, (FileSplit) split);
	}

	protected long computeSplitSize(long goalSize, long minSize,
			long blockSize) {
		return blockSize;
	}

}
