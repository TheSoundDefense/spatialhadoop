package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Serialized text reocrd reader.
 * A record reader that reads objects of type TextSerializable. Since the text
 * serialization of the object does not contains its type, the type should
 * be set explicitly by overriding the method createValue.
 * The key is the offset in file and the value is the text deserialized object.
 * @author eldawy
 *
 */
public abstract class STextRecordReader<T extends TextSerializable>
  implements RecordReader<LongWritable, T> {
  
  /**An underlying reader to read lines of the text file*/
  private RecordReader<LongWritable, Text> lineRecordReader;
  private Text subValue;

  public STextRecordReader(Configuration job, FileSplit split)
      throws IOException {
    lineRecordReader = new LineRecordReader(job, split);
    subValue = lineRecordReader.createValue();
  }

  public STextRecordReader(InputStream in, long offset, long endOffset) {
    lineRecordReader = new LineRecordReader(in, offset, endOffset, 8192);
    subValue = lineRecordReader.createValue();
  }

  @Override
  public boolean next(LongWritable key, T value) throws IOException {
    if (!lineRecordReader.next(key, subValue) || subValue.getLength() == 0) {
      // Stop on wrapped reader EOF or an empty line which indicates EOF too
      return false;
    }
    // Convert to a regular string to be able to use split
    value.fromText(subValue);

    return true;
  }

  @Override
  public void close() throws IOException {
    lineRecordReader.close();
  }
  
  @Override
  public long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override
  public LongWritable createKey() {
    return lineRecordReader.createKey();
  }

}
