package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridRecordWriter;

/**
 * Reads a random sample of a file.
 * @author eldawy
 *
 */
public class Sampler {

  private static final int MAX_LINE_LENGTH = 4096;

  /**
   * Reads a sample of the given file and returns the number of items
   * read.
   * @param fs
   * @param file
   * @param count
   * @return
   * @throws IOException 
   */
  static <T extends Shape> int sampleLocal(FileSystem fs, Path file, int count,
      OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    long file_length = fs.getFileStatus(file).getLen();
    FSDataInputStream in = fs.open(file);
    int records_read = 0;
    
    if (in.readLong() == RTreeGridRecordWriter.RTreeFileMarker) {
      // Sampling from an RTree
      LongWritable elementId = new LongWritable();
      RTree<T> rtree = new RTree<T>();
      rtree.setStockObject(stockObject);
      
      long block_size = fs.getFileStatus(file).getBlockSize();
      int block_count = (int) (file_length / block_size);
      int i_block = 0;
      while (records_read < count) {
        // Skip to next block
        long pos = in.getPos();
        in.skip((block_size - (pos - 8) % block_size)%block_size);
        rtree.readFields(in);
        
        int limit = ++i_block * count / block_count;
        // Read items in order to ensure one pass over records
        int ids[] = new int[limit - records_read];
        for (int i = 0; i < ids.length; i++) {
          ids[i] = (int) (Math.random() * rtree.getElementCount());
        }
        Arrays.sort(ids);
        Iterator<T> iter = rtree.iterator();
        int current_pos = 0;
        for (int i = 0; i < ids.length; i++) {
          while (current_pos < ids[i]) {
            current_pos++;
            iter.next();
          }
          stockObject = iter.next();
          if (output != null) {
            elementId.set(ids[i]);
            output.collect(elementId, stockObject);
          }
          records_read++;
        }
      }
    } else {
      // Sampling a non-local-indexed file (text lines)
      LongWritable line_offset = new LongWritable();
      Text line = new Text();
      byte[] line_bytes = new byte[MAX_LINE_LENGTH];

      // Reads items in order to ensure records in the same block are read
      // together
      long[] poss = new long[count];
      for (int i = 0; i < poss.length; i++) {
        poss[i] = (long) (Math.random() * file_length);
      }
      Arrays.sort(poss);
      
      for (int i = 0; i < count; i++) {
        int line_length = 0;
        in.seek(poss[i]);
        // Skip until the first end of line
        // Skip the rest of this line
        do {
          line_bytes[line_length] = in.readByte();
        } while (in.available() > 0 && line_bytes[line_length] != '\n' && line_bytes[line_length] != '\r');
        
        // Skip if that was the last line in file
        if (in.available() == 0)
          continue;
        
        line_offset.set(in.getPos());
        
        do {
          line_bytes[line_length] = in.readByte();
          if (line_bytes[line_length] == '\n' || line_bytes[line_length] == '\r')
            break;
          line_length++;
        } while (in.available() > 0);
        
        if (output != null) {
          line.set(line_bytes, 0, line_length);
          stockObject.fromText(line);
          output.collect(line_offset, stockObject);
        }
        records_read++;
      }
    }
    in.close();
    return records_read;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Sampler.class);
    Path inputFile = cla.getPath();
    FileSystem fs = inputFile.getFileSystem(conf);
    int count = cla.getCount();
    long record_count = sampleLocal(fs, inputFile, count, new OutputCollector<LongWritable, TigerShape>() {
      @Override
      public void collect(LongWritable key, TigerShape value)
          throws IOException {
        //System.out.println(key+": "+value);
      }
    }, new TigerShape());
    System.out.println("Sampled "+record_count+" records");
  }
}
