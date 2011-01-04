package spatial.mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * Reads data from a CombineFileRecordReader.
 * It emits records where key is K and value is a collection of V.
 * The value is the collected values from all files in the input split.
 * @author aseldawy
 *
 * @param <K>
 * @param <V>
 */
public class MergeFileRecordReader<K extends Writable, V extends Writable> implements RecordReader<K, CollectionWritable<V>> {
	private CombineFileRecordReader<K, V> combineFileRecordReader;
	private final CombineFileSplit split;
	
	public MergeFileRecordReader(
			JobConf job,
			CombineFileSplit split,
			Reporter reporter,
			Class<RecordReader<K, V>> rrClass)
			throws IOException {
		this.split = split;
		this.combineFileRecordReader = new CombineFileRecordReader<K, V>(
				job, (CombineFileSplit) split, reporter, rrClass);
	}

	@Override
	public boolean next(K key, CollectionWritable<V> value) throws IOException {
		boolean success = true;
		value.clear();
		for (int i = 0; i < split.getNumPaths(); i++) {
			V v = combineFileRecordReader.createValue();
			// Result is success when all records are read successfully
			success = success && combineFileRecordReader.next(key, v);
			value.add(v);
		}
		return success;
	}

	@Override
	public K createKey() {
		return combineFileRecordReader.createKey();
	}

	@Override
	public CollectionWritable<V> createValue() {
		return new ArrayListWritable<V>();
	}

	@Override
	public long getPos() throws IOException {
		return combineFileRecordReader.getPos();
	}

	@Override
	public void close() throws IOException {
		combineFileRecordReader.close();
	}

	@Override
	public float getProgress() throws IOException {
		return combineFileRecordReader.getProgress();
	}

}
