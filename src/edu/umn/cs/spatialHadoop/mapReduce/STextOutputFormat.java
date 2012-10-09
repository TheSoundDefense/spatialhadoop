package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Writes objects using {@link TextSerializable#toText(org.apache.hadoop.io.Text)}
 * method if supported. The key is written using {@link Object#toString()}
 * but the value is written using
 * {@link TextSerializable#toText(org.apache.hadoop.io.Text)}
 * @author eldawy
 *
 */
public class STextOutputFormat<K, V extends TextSerializable>
    extends TextOutputFormat<K, V> {

  protected static class LineRecordWriter<K, V extends TextSerializable>
  implements RecordWriter<K, V> {
    /**An internal writer that writes text*/
    RecordWriter<K, Text> internalWriter; 
    
    /**A temporary text used to serialize the value*/
    private final Text text = new Text();

    public LineRecordWriter(DataOutputStream out, String separator) {
      internalWriter =
          new TextOutputFormat.LineRecordWriter<K, Text>(out, separator);
    }

    public LineRecordWriter(DataOutputStream out) {
      internalWriter =
          new TextOutputFormat.LineRecordWriter<K, Text>(out);
    }

    public synchronized void write(K key, V value) throws java.io.IOException {
      value.toText(text);
      internalWriter.write(key, text);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      internalWriter.close(reporter);
    }
  }
  
  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator = job.get("mapred.textoutputformat.separator", 
                                       "\t");
    if (!isCompressed) {
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
    } else {
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(job, GzipCodec.class);
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
      // build the filename including the extension
      Path file = 
        FileOutputFormat.getTaskOutputPath(job, 
                                           name + codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LineRecordWriter<K, V>(new DataOutputStream
                                        (codec.createOutputStream(fileOut)),
                                        keyValueSeparator);
    }
  }
}
