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
    LongWritable elementId = new LongWritable(); // Key for returned elements
    
    // Generate offsets to read from and make sure they are ordered to minimze
    // seeks between different HDFS blocks
    long[] positions = new long[count];
    for (int i = 0; i < positions.length; i++) {
      positions[i] = (long) (Math.random() * file_length);
    }
    
    Arrays.sort(positions);
    long block_size = fs.getFileStatus(file).getBlockSize();
    
    while (records_read < count) {
      long block_start_offset = positions[records_read] -
          (positions[records_read]  % block_size);
      // Seek to this block and check its type
      in.seek(block_start_offset);
      if (in.readLong() == RTreeGridRecordWriter.RTreeFileMarker) {
        // This block is an RTree block
        RTree<T> rtree = new RTree<T>();
        rtree.setStockObject(stockObject);
        rtree.readFields(in);

        Iterator<T> iter = rtree.iterator();
        int iterator_index = 0;
        
        // Consider all positions in this block
        while (records_read < count &&
            positions[records_read] < block_start_offset + block_size) {
          // Map file position to element index in this tree assuming fixed
          // size records
          int element_index = (int) ((positions[records_read] - block_start_offset) *
              rtree.getElementCount() / block_size);
          while (iterator_index++ < element_index)
            iter.next();
          stockObject = iter.next();
          if (output != null) {
            elementId.set(element_index);
            output.collect(elementId, stockObject);
          }
          records_read++;
        }
      } else {
        // Non local-indexed file
        // It might be a part of a grid file and the block is stuffed with
        // empty new lines at the end.
        // We need first to find the last offset in the block that has real
        // data (not empty lines).
        // We perform an exponential search starting from the last offset in
        // block
        long last_non_empty_offset = Math.min(block_start_offset + block_size,
            file_length);
        int check_offset = 1;
        while (check_offset < block_size) {
          in.seek(last_non_empty_offset - check_offset * 2);
          byte b1 = in.readByte();
          byte b2 = in.readByte();
          if (b1 != '\n' || b2 != '\n') {
            // We found a non-empty line. Perform a binary search till we find
            // the last non-empty line
            long l = last_non_empty_offset - check_offset * 2;
            long h = last_non_empty_offset - check_offset;
            while (l < h) {
              long m = (l + h) / 2;
              in.seek(m);
              b1 = in.readByte(); b2 = in.readByte();
              if (b1 == '\n' && b2 == '\n') {
                // This is an empty line, check before that
                h = m-1;
              } else {
                // This is a non-empty line, check after that
                l = m+1;
              }
            }
            last_non_empty_offset = l;
            break;
          }
          check_offset *= 2;
        }
        
        long block_fill_size = last_non_empty_offset - block_start_offset;
        // Consider all positions in this block
        while (records_read < count &&
            positions[records_read] < block_start_offset + block_size) {
          // Map file position to element index in this tree assuming fixed
          // size records
          long element_offset = (positions[records_read] - block_start_offset) *
              block_fill_size / block_size;
          in.seek(element_offset);
          // Read the first line after that offset
          Text line = new Text();
          byte[] line_bytes = new byte[MAX_LINE_LENGTH];
          
          // Skip the rest of this line
          int line_length = 0;
          do {
            line_bytes[line_length] = in.readByte();
          } while (in.getPos() < last_non_empty_offset && line_bytes[line_length] != '\n' && line_bytes[line_length] != '\r');
          
          elementId.set(in.getPos());
          
          do {
            line_bytes[line_length] = in.readByte();
            if (line_bytes[line_length] == '\n' || line_bytes[line_length] == '\r')
              break;
            line_length++;
          } while (in.getPos() < last_non_empty_offset);
          
          // Skip very short lines (probably an empty line)
          if (line_length < 5)
            continue;
          
          if (output != null) {
            line.set(line_bytes, 0, line_length);
            stockObject.fromText(line);
            output.collect(elementId, stockObject);
          }
          records_read++;
        }
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
//        System.out.println(key+": "+value);
      }
    }, new TigerShape());
    System.out.println("Sampled "+record_count+" records");
  }
}
