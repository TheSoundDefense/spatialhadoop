package edu.umn.cs.spatial.mapReduce;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.spatial.TigerShapeWithIndex;

/**
 * Reads and parses a file that contains records of type Rectangle.
 * Records are assumed to be fixed size and of the format
 * <id>,<left>,<top>,<right>,<bottom>
 * When a record of all zeros is encountered, it is assumed to be the end of file.
 * This means, no more records are processed after a zero-record.
 * Records are read one be one.
 * @author aseldawy
 *
 */
public class SJInputFormat extends FileInputFormat<LongWritable, TigerShapeWithIndex> {

	@Override
	public RecordReader<LongWritable, TigerShapeWithIndex> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
	    reporter.setStatus(split.toString());
    FileStatus[] files = listStatus(job);
    int i = 0;
    while (i < files.length && !files[i].getPath().equals(((FileSplit)split).getPath()))
      i++;
		return new TigerRectangleWithIndexRecordReader(job, (FileSplit) split, i);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	  // Set grid info in SJMapReduce
	  String gridInfoStr = job.get(SJMapReduce.GRID_INFO);
	  String[] parts = gridInfoStr.split(",");
	  SJMapReduce.gridInfo = new GridInfo();
	  SJMapReduce.gridInfo.xOrigin = Long.parseLong(parts[0]);
	  SJMapReduce.gridInfo.yOrigin = Long.parseLong(parts[1]);
	  SJMapReduce.gridInfo.gridWidth = Long.parseLong(parts[2]);
	  SJMapReduce.gridInfo.gridHeight = Long.parseLong(parts[3]);
	  SJMapReduce.gridInfo.cellWidth = Long.parseLong(parts[4]);
	  SJMapReduce.gridInfo.cellHeight = Long.parseLong(parts[5]);
	  return super.getSplits(job, numSplits);
	}
}
