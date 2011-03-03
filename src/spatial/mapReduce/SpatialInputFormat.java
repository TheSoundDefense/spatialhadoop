package spatial.mapReduce;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

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
public class SpatialInputFormat extends MultiFileInputFormat<Rectangle, CollectionWritable<CollectionWritable<Rectangle>>> {

	@Override
	public RecordReader<Rectangle, CollectionWritable<CollectionWritable<Rectangle>>> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
		@SuppressWarnings("unchecked")
		Class<RecordReader<Rectangle, CollectionWritable<Rectangle>>> klass =
			(Class<RecordReader<Rectangle, CollectionWritable<Rectangle>>>) SpatialRecordReader.class
				.asSubclass(RecordReader.class);
	    //return new SpatialRecordReader(job, (FileSplit) split);
		return new MergeFileRecordReader<Rectangle, CollectionWritable<Rectangle>>(
				job, (CombineFileSplit) split, reporter, klass);
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) 
	throws IOException {

		Path[] paths = FileUtil.stat2Paths(listStatus(job));
		
		long blockSize = paths[0].getFileSystem(job).getFileStatus(paths[0]).getBlockSize();
		long totalSize = paths[0].getFileSystem(job).getFileStatus(paths[0]).getLen();
		// Be sure that all files are of the same block size and total size
		for (int i = 1; i < paths.length; i++) {
			FileSystem fs1 = paths[i-1].getFileSystem(job);
			FileStatus file1 = fs1.getFileStatus(paths[i-1]);
			FileSystem fs2 = paths[i].getFileSystem(job);
			FileStatus file2 = fs2.getFileStatus(paths[i]);
			if (file1.getBlockSize() != file2.getBlockSize() ||
					file1.getLen() != file2.getLen()) {
				throw new RuntimeException("Files must be of equal size and equal block size");
			}
			// Use block size as split size
			blockSize = file1.getBlockSize();
			totalSize = file1.getLen();
		}
		
		numSplits = (int) Math.ceil((double)totalSize / blockSize);
		CombineFileSplit[] splits = new CombineFileSplit[numSplits];
		
		for (int i = 0; i < numSplits; i++) {
			long[] start = new long[paths.length];
			long[] lengths = new long[paths.length];
			String[] locations = new String[paths.length];
			for (int p = 0; p < paths.length; p++) {
				start[p] = i * blockSize;
				lengths[p] = Math.min(blockSize, totalSize - start[p]); 
				// Initialize locations like in org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
				locations[p] = "";
			}
			splits[i] = new CombineFileSplit(job, paths, start, lengths, locations);
		}
		return splits;
	}

}
