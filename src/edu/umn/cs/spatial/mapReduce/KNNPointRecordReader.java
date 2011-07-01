package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import edu.umn.edu.spatial.Point;


/**
 * Parses a spatial file containing one point in each line.
 * @author aseldawy
 *
 */
public class KNNPointRecordReader implements RecordReader<LongWritable, Point> {
  
  private RecordReader<LongWritable, Text> lineRecordReader;
  private LongWritable subKey;
  private Text subValue;

  public KNNPointRecordReader(Configuration job, 
      FileSplit split) throws IOException {
    lineRecordReader = new LineRecordReader(job, split);
  }

  /**
	 * Reads a rectangle and emits it.
	 * It skips bytes until end of line is found.
	 * After this, it starts reading rectangles with a line in each rectangle.
	 * It consumes the end of line also when reading the rectangle.
	 * It stops after reading the first end of line (after) end.
	 */
	public boolean next(LongWritable key, Point value) throws IOException {
	  if (!lineRecordReader.next(subKey, subValue) || subValue.getLength() < 4) {
	    // Stop on wrapped reader EOF or a very short line which indicates EOF too
	    return false;
	  }
	  // Convert to a regular string to be able to use split
	  String line = new String(subValue.getBytes(), 0, subValue.getLength());
	  String[] parts = line.split(",");
	  key.set(Long.parseLong(parts[0]));
	  value.id = key.get();
	  value.x = Integer.parseInt(parts[1]);
	  value.y = Integer.parseInt(parts[2]);
	  value.type = Integer.parseInt(parts[3]);

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
    subKey = lineRecordReader.createKey();
    return new LongWritable();
  }

  @Override
  public Point createValue() {
    subValue = lineRecordReader.createValue();
    return new Point();
  }

}
