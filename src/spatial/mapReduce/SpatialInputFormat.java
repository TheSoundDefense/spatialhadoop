package spatial.mapReduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
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
public class SpatialInputFormat extends FileInputFormat<Rectangle, CollectionWritable<CollectionWritable<Rectangle>>> {

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
		InputSplit[] splits = super.getSplits(job, numSplits);
		// Sort by start index of splits
		Arrays.sort(splits, new Comparator<InputSplit>() {
			@Override
			public int compare(InputSplit o1, InputSplit o2) {
				return (int)(((FileSplit)o1).getStart() - ((FileSplit)o2).getStart());
			}
		});

		// Merge each run of splits from the same locations into one split
		ArrayList<CombineFileSplit> mergedSplits = new ArrayList<CombineFileSplit>();
		// i1, i2 are indexes for begin and end of current run
		int i1 = 0;
		int i2 = 1;
		while (i2 <= splits.length) {
			FileSplit fileSplit1 = (FileSplit) splits[i1];
			FileSplit fileSplit2 = i2 < splits.length ? (FileSplit) splits[i2] : null;
			if (i2 == splits.length ||
					fileSplit1.getStart() != fileSplit2.getStart()) {
				// There's a run from i1 to i2-1
				Path[] files = new Path[i2-i1];
				long[] start = new long[i2-i1];
				long[] lengths = new long[i2-i1];
				String[] locations = new String[i2-i1];
				for (int i=0; i < i2-i1; i++) {
					files[i] = ((FileSplit)splits[i+i1]).getPath();
					start[i] = ((FileSplit)splits[i+i1]).getStart();
					lengths[i] = ((FileSplit)splits[i+i1]).getLength();
					// TODO merge arrays of locations into one big array
					locations[i] = ((FileSplit)splits[i+i1]).getLocations()[0];
				}
				mergedSplits.add(new CombineFileSplit(job, files, start, lengths, locations));
				i1 = i2;
			}
			i2++;
		}
		return mergedSplits.toArray(new CombineFileSplit[mergedSplits.size()]);
	}

	protected long computeSplitSize(long goalSize, long minSize,
			long blockSize) {
		return blockSize;
	}

}
