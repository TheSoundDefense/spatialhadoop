package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

import edu.umn.cs.spatialHadoop.PointWithK;

public class KNNOutputCommitter extends FileOutputCommitter {
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    // Set QueryRange in the mapper class
    String queryRangeStr = context.getConfiguration().get(KNNMapReduce.QUERY_POINT);
    KNNMapReduce.queryPoint = new PointWithK();
    KNNMapReduce.queryPoint.readFromString(queryRangeStr);

    super.setupTask(context);
  }
}
