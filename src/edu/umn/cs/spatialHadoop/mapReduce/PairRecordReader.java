package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;


/**
 * Reads every possible pair of values from two underlying record readers.
 * In other words, it makes a Cartesian product from the records stored in
 * the two splits given to this reader.
 * @author eldawy
 *
 * @param <K>
 * @param <V>
 */
public abstract class PairRecordReader<K extends WritableComparable, V extends Writable>
    implements RecordReader<PairWritableComparable<K>, PairWritable<V>> {
  
  /**The internal readers that actually do the parsing*/
  protected Pair<RecordReader<K, V>> internalReaders;
  
  /**The two splits parsed by this record reader*/
  protected PairOfFileSplits splits;
  
  /**Configuration of the current job*/
  protected Configuration conf;
  
  /**
   * Creates a record reader for one of the two splits parsed by this reader.
   * @param split
   * @return
   */
  protected abstract RecordReader<K, V> createRecordReader(Configuration conf,
      FileSplit split) throws IOException;
  
  @Override
  public boolean next(PairWritableComparable<K> key, PairWritable<V> value) throws IOException {
    if (internalReaders.second.next(key.second, value.second)) {
      return true;
    }
    // Reached the end of the second split. Reset the second split and advance
    // to the next item in the first split
    if (!internalReaders.first.next(key.first, value.first)) {
      return false;
    }
    internalReaders.second.close();
    internalReaders.second = createRecordReader(conf, splits.second);
    return internalReaders.second.next(key.second, value.second);
  }

  @Override
  public PairWritableComparable<K> createKey() {
    PairWritableComparable<K> key = new PairWritableComparable<K>();
    key.first = internalReaders.first.createKey();
    key.second = internalReaders.second.createKey();
    return key;
  }

  @Override
  public PairWritable<V> createValue() {
    PairWritable<V> value = new PairWritable<V>();
    value.first = internalReaders.first.createValue();
    value.second = internalReaders.second.createValue();
    return value;
  }

  @Override
  public long getPos() throws IOException {
    return internalReaders.first.getPos() + internalReaders.second.getPos();
  }

  @Override
  public void close() throws IOException {
    internalReaders.first.close();
    internalReaders.second.close();
  }

  @Override
  public float getProgress() throws IOException {
    return (internalReaders.first.getProgress() +
        internalReaders.second.getProgress()) / 2;
  }


}
