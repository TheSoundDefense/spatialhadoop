package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.util.Progressable;

/**
 * A customized version of GridOutputFormat that sets cells in
 * RepartitionMapReduce class first
 * @author eldawy
 *
 */
public class RPGridOutputFormat extends GridOutputFormat {
  @Override
  public RecordWriter<CellInfo, Text> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
    RepartitionMapReduce.setCellInfos(decodeCells(job.get(OUTPUT_CELLS)));
    return super.getRecordWriter(ignored, job, name, progress);
  }
}
