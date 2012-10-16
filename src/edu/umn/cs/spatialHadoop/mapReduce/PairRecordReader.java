package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;

/**
 * 
 * @author eldawy
 *
 * @param <K>
 * @param <V>
 */
public class PairRecordReader<K, V extends Shape> implements RecordReader<K, Pair<RTree<V>>> {

  @Override
  public boolean next(K key, Pair<RTree<V>> value) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public K createKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Pair<RTree<V>> createValue() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public float getProgress() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }


}
