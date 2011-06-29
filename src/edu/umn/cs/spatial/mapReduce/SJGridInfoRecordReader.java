package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.spatial.GridInfo;


/**
 * Parses a spatial file and returns a list of <id, rectangle> tuples.
 * Used with range query (spatial selection)
 * @author aseldawy
 *
 */
public class SJGridInfoRecordReader implements RecordReader<LongWritable, GridInfo> {
  
  private RecordReader<LongWritable, Text> lineRecordReader;
  private Text subValue;

  public SJGridInfoRecordReader(Configuration job, 
      FileSplit split) throws IOException {
    lineRecordReader = new LineRecordReader(job, split);
  }

  /**
	 * Reads a line from the underlying line reader and emits its value.
	 */
	public boolean next(LongWritable key, GridInfo value) throws IOException {
	  if (!lineRecordReader.next(key, subValue) || subValue.getLength() < 4) {
	    // Stop on wrapped reader EOF or a very short line which indicates EOF too
	    return false;
	  }
	  // Convert to a regular string to be able to use split
	  String line = new String(subValue.getBytes(), 0, subValue.getLength());
	  String[] parts = line.split(",");
	  key.set(Integer.parseInt(parts[0]));
    value.xOrigin = Integer.parseInt(parts[0]);
    value.yOrigin = Integer.parseInt(parts[1]);
    value.gridWidth = Integer.parseInt(parts[2]);
    value.gridHeight = Integer.parseInt(parts[3]);
    value.cellWidth = Integer.parseInt(parts[4]);
    value.cellHeight = Integer.parseInt(parts[5]);

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
  public GridInfo createValue() {
    subValue = lineRecordReader.createValue();
    return new GridInfo();
  }

}
