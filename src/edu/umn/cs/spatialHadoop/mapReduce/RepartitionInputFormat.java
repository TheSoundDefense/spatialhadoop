package edu.umn.cs.spatialHadoop.mapReduce;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.TigerShape;




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
public class RepartitionInputFormat extends FileInputFormat<LongWritable, TigerShape> {

  @Override
  public RecordReader<LongWritable, TigerShape> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {

    GridInfo gridInfo = new GridInfo();
    RepartitionMapReduce.gridInfo = gridInfo;
    gridInfo.readFromString(job.get(GridOutputFormat.OUTPUT_GRID));
    RepartitionMapReduce.cellInfos = new CellInfo[gridInfo.getGridColumns()][gridInfo
        .getGridRows()];
    for (int col = 0; col < gridInfo.getGridColumns(); col++) {
      for (int row = 0; row < gridInfo.getGridRows(); row++) {
        RepartitionMapReduce.cellInfos[col][row] = new CellInfo(col
            * gridInfo.cellWidth + gridInfo.xOrigin, row * gridInfo.cellHeight
            + gridInfo.yOrigin, gridInfo.cellWidth, gridInfo.cellHeight);
      }
    }
    reporter.setStatus(split.toString());
    return new TigerShapeRecordReader(job, (FileSplit)split);
  }

}
