package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import edu.umn.cs.spatial.TigerShapeWithIndex;


/**
 * Parses text lines into Tiger shapes. Subclasses of this class should provide
 * a method to parse one line into a shape.
 * @author aseldawy
 *
 */
public class TigerShapeWithIndexRecordReader implements RecordReader<LongWritable, TigerShapeWithIndex> {
  
  private RecordReader<LongWritable, Text> lineRecordReader;
  private Text subValue;
  private int index;

  public TigerShapeWithIndexRecordReader(Configuration job, FileSplit split, int index)
      throws IOException {
    lineRecordReader = new LineRecordReader(job, split);
    this.index = index;
  }

  /**
	 * Reads a rectangle and emits it.
	 * It skips bytes until end of line is found.
	 * After this, it starts reading rectangles with a line in each rectangle.
	 * It consumes the end of line also when reading the rectangle.
	 * It stops after reading the first end of line (after) end.
	 */
	public boolean next(LongWritable key, TigerShapeWithIndex value) throws IOException {
	  if (!lineRecordReader.next(key, subValue) || subValue.getLength() < 4) {
	    // Stop on wrapped reader EOF or a very short line which indicates EOF too
	    return false;
	  }
    // Convert to a regular string to be able to use split
    String line = new String(subValue.getBytes(), 0, subValue.getLength());
    value.readFromString(line);
	  
	  return true;
	}

  public long getPos() throws IOException {
    return lineRecordReader.getPos();
  }

  public void close() throws IOException {
    lineRecordReader.close();
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }

  @Override
  public LongWritable createKey() {
    return lineRecordReader.createKey();
  }

  @Override
  public TigerShapeWithIndex createValue() {
    subValue = lineRecordReader.createValue();
    TigerShapeWithIndex tigerShape = new TigerShapeWithIndex();
    tigerShape.index = index;
    return tigerShape;
  }

}
