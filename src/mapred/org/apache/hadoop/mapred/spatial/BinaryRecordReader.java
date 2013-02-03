package org.apache.hadoop.mapred.spatial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;



/**
 * Reads every possible pair of values from two underlying record readers.
 * In other words, it makes a Cartesian product from the records stored in
 * the two splits given to this reader.
 * @author eldawy
 *
 * @param <K>
 * @param <V>
 */
public abstract class BinaryRecordReader<K extends Writable, V extends Writable>
    implements RecordReader<PairWritable<K>, PairWritable<V>> {
  
  /**A flag that is set before the first record is read*/
  protected boolean firstTime = true;
  
  
  /**The internal readers that actually do the parsing*/
  protected RecordReader<K, V>[] internalReaders;
  
  /**The two splits parsed by this record reader*/
  protected CombineFileSplit split;
  
  /**Configuration of the current job*/
  protected Configuration conf;
  
  /**
   * Creates a record reader for one of the two splits parsed by this reader.
   * @param split
   * @return
   */
  protected abstract RecordReader<K, V> createRecordReader(Configuration conf,
      CombineFileSplit split, int index) throws IOException;
  
  @SuppressWarnings("unchecked")
  public BinaryRecordReader(Configuration conf, CombineFileSplit split) throws IOException {
    this.conf = conf;
    this.split = split;
    internalReaders = new RecordReader[(int) split.getLength()];
    // Initialize all record readers
    for (int i = 0; i < split.getNumPaths(); i++) {
      this.internalReaders[i] = createRecordReader(this.conf, this.split, i);
    }
  }
  
  @Override
  public boolean next(PairWritable<K> key, PairWritable<V> value) throws IOException {
    if (firstTime) {
      if (!internalReaders[0].next(key.first, value.first)) {
        return false;
      }
      firstTime = false;
    }
    if (internalReaders[1].next(key.second, value.second)) {
      return true;
    }
    // Reached the end of the second split. Reset the second split and advance
    // to the next item in the first split
    if (!internalReaders[0].next(key.first, value.first)) {
      return false;
    }
    internalReaders[1].close();
    internalReaders[1] = createRecordReader(conf, split, 1);
    return internalReaders[1].next(key.second, value.second);
  }

  @Override
  public PairWritable<K> createKey() {
    PairWritable<K> key = new PairWritable<K>();
    key.first = internalReaders[0].createKey();
    key.second = internalReaders[1].createKey();
    return key;
  }

  @Override
  public PairWritable<V> createValue() {
    PairWritable<V> value = new PairWritable<V>();
    value.first = internalReaders[0].createValue();
    value.second = internalReaders[1].createValue();
    return value;
  }

  @Override
  public long getPos() throws IOException {
    return internalReaders[0].getPos() + internalReaders[1].getPos();
  }

  @Override
  public void close() throws IOException {
    internalReaders[0].close();
    internalReaders[1].close();
  }

  @Override
  public float getProgress() throws IOException {
    return (internalReaders[0].getProgress() +
        internalReaders[1].getProgress()) / 2;
  }
}
