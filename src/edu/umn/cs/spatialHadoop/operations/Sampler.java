package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text2;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.SpatialSite;

import edu.umn.cs.CommandLineArguments;

/**
 * Reads a random sample of a file.
 * @author eldawy
 *
 */
public class Sampler {

  private static final int MAX_LINE_LENGTH = 4096;
  
  /**
   * Records as many records as wanted until the total size of the text
   * serialization of sampled records exceed the given limit
   * @param fs
   * @param files
   * @param total_size
   * @param output
   * @param stockObject
   * @return
   * @throws IOException
   */
  public static <T extends TextSerializable> int sampleLocalWithSize(
      FileSystem fs, Path[] files, long total_size,
      final OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    int average_record_size = 40; // A wild guess for record size
    final LongWritable current_sample_size = new LongWritable();
    final Text text = new Text();
    int sample_count = 0;
    while (current_sample_size.get() < total_size) {
      int count = (int) ((total_size - current_sample_size.get()) / average_record_size);
      if (count < 10)
        count = 10;
      sample_count += sampleLocal(fs, files, count, new OutputCollector<LongWritable, T>() {
        @Override
        public void collect(LongWritable key, T value) throws IOException {
          text.clear();
          value.toText(text);
          current_sample_size.set(current_sample_size.get() + text.getLength());
          if (output != null)
            output.collect(key, value);
        }
      } , stockObject);
      // Update average_records_size
      average_record_size = (int) (current_sample_size.get() / sample_count);
    }
    return sample_count;
  }

  public static <T extends TextSerializable> int sampleLocal(FileSystem fs, Path file, int count,
      OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    return sampleLocal(fs, new Path[] {file}, count, output, stockObject);
  }
  
  /**
   * Reads a sample of the given file and returns the number of items
   * read.
   * @param fs
   * @param file
   * @param count
   * @return
   * @throws IOException 
   */
  public static <T extends TextSerializable> int sampleLocal(FileSystem fs, Path[] files, int count,
      OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    long[] files_start_offset = new long[files.length+1]; // Prefix sum of files sizes
    long total_length = 0;
    for (int i_file = 0; i_file < files.length; i_file++) {
      files_start_offset[i_file] = total_length;
      total_length += fs.getFileStatus(files[i_file]).getLen();
    }
    files_start_offset[files.length] = total_length;
    
    // Generate offsets to read from and make sure they are ordered to minimize
    // seeks between different HDFS blocks
    long[] offsets = new long[count];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = (long) (Math.random() * total_length);
    }
    Arrays.sort(offsets);

    LongWritable elementId = new LongWritable(); // Key for returned elements
    int record_i = 0; // Number of records read so far
    int records_returned = 0;
    
    int file_i = 0; // Index of the current file being sampled
    while (record_i < count) {
      // Skip to the file that contains the next sample
      while (offsets[record_i] > files_start_offset[file_i+1])
        file_i++;

      // Open a stream to the current file and use it to read all samples
      // in this file
      FSDataInputStream current_file_in = fs.open(files[file_i]);
      long current_file_size = files_start_offset[file_i+1] - files_start_offset[file_i];
      long current_file_block_size = fs.getFileStatus(files[file_i]).getBlockSize();
      
      // Keep sampling as long as records offsets are within this file
      while (record_i < count &&
          (offsets[record_i] -= files_start_offset[file_i]) < current_file_size) {
        long current_block_start_offset = offsets[record_i] -
            (offsets[record_i] % current_file_block_size);
        // Seek to this block and check its type
        current_file_in.seek(current_block_start_offset);
        // The start and end offsets of data within this block
        // offsets are calculated relative to file start
        long data_start_offset = current_block_start_offset;
        if (current_file_in.readLong() == SpatialSite.RTreeFileMarker) {
          // This block is an RTree block. Update the start offset to point
          // to the first byte after the header
          data_start_offset =
              current_block_start_offset + RTree.getHeaderSize(current_file_in);
        } 
        // Get the end offset of data by searching for the last non-empty line
        // We perform an exponential search starting from the last offset in
        // block. This assumes that all empty lines occur at the end
        long data_end_offset = Math.min(current_block_start_offset
            + current_file_block_size, current_file_size);
        int check_offset = 1;
        while (check_offset < current_file_block_size) {
          current_file_in.seek(data_end_offset - check_offset * 2);
          byte b1 = current_file_in.readByte();
          byte b2 = current_file_in.readByte();
          if (b1 != '\n' || b2 != '\n') {
            // We found a non-empty line. Perform a binary search till we find
            // the last non-empty line
            long l = data_end_offset - check_offset * 2;
            long h = data_end_offset - check_offset;
            while (l < h) {
              long m = (l + h) / 2;
              current_file_in.seek(m);
              b1 = current_file_in.readByte(); b2 = current_file_in.readByte();
              if (b1 == '\n' && b2 == '\n') {
                // This is an empty line, check before that
                h = m-1;
              } else {
                // This is a non-empty line, check after that
                l = m+1;
              }
            }
            // Subtract 100 to ensure the mapped offset doesn't fall
            // in the last line
            data_end_offset = l - 100;
            break;
          }
          check_offset *= 2;
        }

        long block_fill_size = data_end_offset - data_start_offset;

        // Consider all positions in this block
        while (record_i < count &&
            offsets[record_i] < current_block_start_offset + current_file_block_size) {
          // Map file position to element index in this tree assuming fixed
          // size records
          long element_offset_in_block =
              (offsets[record_i] - current_block_start_offset) *
              block_fill_size / current_file_block_size;
          current_file_in.seek(data_start_offset + element_offset_in_block);
          // Read the first line after that offset
          Text line = new Text();
          byte[] line_bytes = new byte[MAX_LINE_LENGTH];

          // Skip the rest of this line
          int line_length = 0;
          do {
            line_bytes[line_length] = current_file_in.readByte();
          } while (current_file_in.getPos() < data_end_offset &&
              (line_bytes[line_length] != '\n' && line_bytes[line_length] != '\r'));

          elementId.set(files_start_offset[file_i] + current_file_in.getPos());

          do {
            line_bytes[line_length] = current_file_in.readByte();
            if (line_bytes[line_length] == '\n' || line_bytes[line_length] == '\r')
              break;
            line_length++;
          } while (current_file_in.getPos() < data_end_offset);

          // Skip empty line
          if (line_length == 0) {
            // Skip this line
            record_i++;
            continue;
          }

          // Report this element to output
          if (output != null) {
            line.set(line_bytes, 0, line_length);
            stockObject.fromText(line);
            output.collect(elementId, stockObject);
          }
          record_i++;
          records_returned++;
        }
      }
      current_file_in.close();
    }
    return records_returned;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Sampler.class);
    Path[] inputFiles = cla.getPaths();
    FileSystem fs = inputFiles[0].getFileSystem(conf);
    int count = cla.getCount();
    long size = cla.getSize();
    TextSerializable stockObject = cla.getShape(true);
    if (stockObject == null)
      stockObject = new Text2();
    
    OutputCollector<LongWritable, TextSerializable> output =
    new OutputCollector<LongWritable, TextSerializable>() {
      @Override
      public void collect(LongWritable key, TextSerializable value)
          throws IOException {
        System.out.println(key+": "+value);
      }
    };
    
    long record_count;
    if (size == 0) {
      record_count = sampleLocal(fs, inputFiles, count, output, stockObject);
    } else {
      record_count = sampleLocalWithSize(fs, inputFiles, size, output, stockObject);
    }
    System.out.println("Sampled "+record_count+" records");
  }
}
