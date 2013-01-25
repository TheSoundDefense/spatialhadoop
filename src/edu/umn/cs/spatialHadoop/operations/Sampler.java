package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MemoryInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text2;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeLineInputFormat;

/**
 * Reads a random sample of a file.
 * @author eldawy
 *
 */
public class Sampler {

  /**Name of the configuration line for sample ratio*/
  private static final String SAMPLE_RATIO =
      "edu.umn.cs.spatialHadoop.operations.Sampler.SampleRatio";
  /**
   * The threshold in number of samples after which the BIG version is used
   */
  private static final int BIG_SAMPLE = 10000;
  
  public static class Map extends MapReduceBase implements
  Mapper<LongWritable, Text, NullWritable, Text> {

    /**Ratio of lines to sample*/
    private double sampleRatio;
    
    /**A dummy key for all keys*/
    private final NullWritable dummyKey = NullWritable.get();
    
    @Override
    public void configure(JobConf job) {
      sampleRatio = job.getFloat(SAMPLE_RATIO, 0.01f);
    }
    
    public void map(LongWritable lineId, Text line,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
            throws IOException {
      if (Math.random() < sampleRatio)
        output.collect(dummyKey, line);
    }
  }

  public static <T extends TextSerializable> int sampleLocalWithRatio(
      FileSystem fs, Path[] files, double ratio,
      final OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    long total_size = 0;
    for (Path file : files) {
      total_size += fs.getFileStatus(file).getLen();
    }
    return sampleLocalWithSize(fs, files, (long) (total_size * ratio), output,
        stockObject);
  }

  /**
   * Sample a ratio of the file through a MapReduce job
   * @param fs
   * @param files
   * @param ratio
   * @param output
   * @param stockObject
   * @return
   * @throws IOException
   */
  public static <T extends TextSerializable> int sampleMapReduceWithRatio(
      FileSystem fs, Path[] files, double ratio,
      final OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = FileSystem.get(job);
    do {
      outputPath = new Path("/"+files[0].getName()+
          ".sample_"+(int)(Math.random()*1000000));
    } while (outFs.exists(outputPath));
    
    job.setJobName("Sample");
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setMapperClass(Map.class);
    job.setFloat(SAMPLE_RATIO, (float) ratio);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(0);
    
    job.setInputFormat(ShapeLineInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeLineInputFormat.setInputPaths(job, files);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    JobClient.runJob(job);
    
    // Read job result
    int result_size = 0;
    if (output != null) {
      LongWritable line_offset = new LongWritable();
      Text line = new Text();
      FileStatus[] results = outFs.listStatus(outputPath);
      
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          try {
            while (lineReader.readLine(line) > 0) {
              stockObject.fromText(line);
              output.collect(line_offset, stockObject);
              result_size++;
            }
          } catch (RuntimeException e) {
            e.printStackTrace();
          }
          lineReader.close();
        }
      }
    }
    
    outFs.delete(outputPath, true);
    
    return result_size;
  }

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
    int average_record_size = 1024; // A wild guess for record size
    final LongWritable current_sample_size = new LongWritable();
    final Text text = new Text();
    int sample_count = 0;
    while (current_sample_size.get() < total_size) {
      int count = (int) ((total_size - current_sample_size.get()) / average_record_size);
      if (count < 10)
        count = 10;
      
      if (count < BIG_SAMPLE) {
        sample_count += sampleLocalByCount(fs, files, count, new OutputCollector<LongWritable, T>() {
          @Override
          public void collect(LongWritable key, T value) throws IOException {
            text.clear();
            value.toText(text);
            current_sample_size.set(current_sample_size.get() + text.getLength());
            if (output != null)
              output.collect(key, value);
          }
        } , stockObject);
      } else {
        sample_count += sampleLocalBig(fs, files, count, new OutputCollector<LongWritable, T>() {
          @Override
          public void collect(LongWritable key, T value) throws IOException {
            text.clear();
            value.toText(text);
            current_sample_size.set(current_sample_size.get() + text.getLength());
            if (output != null)
              output.collect(key, value);
          }
        } , stockObject);
      }
      // Update average_records_size
      average_record_size = (int) (current_sample_size.get() / sample_count);
    }
    return sample_count;
  }

  public static <T extends TextSerializable> int sampleLocal(FileSystem fs, Path file, int count,
      OutputCollector<LongWritable, T> output, T stockObject) throws IOException {
    return sampleLocalByCount(fs, new Path[] {file}, count, output, stockObject);
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
  public static <T extends TextSerializable> int sampleLocalByCount(
      FileSystem fs, Path[] files, int count,
      OutputCollector<LongWritable, T> output, T stockObject)
      throws IOException {
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
        long current_block_end_offset = Math.min(total_length,
            current_block_start_offset + current_file_block_size);
        int current_block_size = (int) (current_block_end_offset - current_block_start_offset);
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
        long data_end_offset = current_block_end_offset;
        int check_offset = 1;
        while (check_offset < current_block_size) {
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
            // Skip the last line too to ensure to ensure that the mapped position
            // will be before some line in the block
            current_file_in.seek(l);
            data_end_offset = Tail.tail(current_file_in, 1, null, null);
            break;
          }
          check_offset *= 2;
        }
        long block_fill_size = data_end_offset - data_start_offset;

        // Consider all positions in this block
        while (record_i < count &&
            offsets[record_i] < current_block_end_offset) {
          // Map file position to element index in this tree assuming fixed
          // size records
          long element_offset_in_block =
              (offsets[record_i] - current_block_start_offset) *
              block_fill_size / current_block_size;
          current_file_in.seek(data_start_offset + element_offset_in_block);
          // Read the first line after that offset
          Text line = new Text();
          LineReader reader = new LineReader(current_file_in, 1024);
          int skipped_bytes = reader.readLine(line, 0); // Skip first line
          elementId.set(current_block_start_offset + element_offset_in_block + skipped_bytes);
          reader.readLine(line); // Read next line

          // Report this element to output
          if (output != null) {
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
  
  /**
   * Reads a relatively big sample of the file.
   * @param fs
   * @param file
   * @param count
   * @return
   * @throws IOException 
   */
  public static <T extends TextSerializable> int sampleLocalBig(FileSystem fs,
      Path[] files, int count, OutputCollector<LongWritable, T> output,
      T stockObject) throws IOException {
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
    // A temporary text to store one line
    Text line = new Text();
    
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
      byte[] block_data = new byte[(int) current_file_block_size];
      
      // Keep sampling as long as records offsets are within this file
      while (record_i < count &&
          (offsets[record_i] -= files_start_offset[file_i]) < current_file_size) {
        long current_block_start_offset = offsets[record_i] -
            (offsets[record_i] % current_file_block_size);
        long current_block_end_offset = Math.min(current_file_size,
            current_block_start_offset + current_file_block_size);
        int current_block_size =
            (int) (current_block_end_offset - current_block_start_offset);

        // Calculate the start and end offset of record data within the block
        // Both offsets are calculated relative to this block
        
        // Initially, start and end are matched with block data boundaries
        // i.e., all the block contains data
        int data_start_offset = 0;
        int data_end_offset = current_block_size;

        // Seek to this block and check its type
        if (current_file_in.getPos() != current_block_start_offset)
          current_file_in.seek(current_block_start_offset);
        
        // Read the whole block in memory
        current_file_in.readFully(block_data, 0, data_end_offset);
        
        // Check if this block is an RTree
        int i = 0;
        while (i < SpatialSite.RTreeFileMarkerB.length &&
            SpatialSite.RTreeFileMarkerB[i] == block_data[i]) {
          i++;
        }

        // If RTree, update data_start_offset to point right after the index
        if (i == SpatialSite.RTreeFileMarkerB.length) {
          DataInputStream temp_is =
              new DataInputStream(new MemoryInputStream(block_data));
          // This block is an RTree block. Update the start offset to point
          // to the first byte after the header
          data_start_offset = RTree.getHeaderSize(temp_is);
        } 
        // Get the end offset of data by searching for the last non-empty line
        // We perform an exponential search starting from the last offset in
        // block. This assumes that all empty lines occur at the end
        int check_offset = 1;
        while (check_offset < data_end_offset) {
          int check_position = data_end_offset - check_offset * 2;
          if (block_data[check_position] != '\n' || block_data[check_position+1] != '\n') {
            // We found a non-empty line. Perform a binary search till we find
            // the last non-empty line
            int l = data_end_offset - check_offset * 2;
            int h = data_end_offset - check_offset;
            while (l < h) {
              int m = (l + h) / 2;
              if (block_data[m] == '\n' && block_data[m+1] == '\n'){
                // This is an empty line, check before that
                h = m-1;
              } else {
                // This is a non-empty line, check after that
                l = m+1;
              }
            }
            // Skip last line to ensure that the mapped offset falls BEFORE
            // the start of the last line not IN the last line
            do {
              l--;
            } while (block_data[l] != '\n' && block_data[l] != '\r');
            data_end_offset = l;
            break;
          }
          check_offset *= 2;
        }

        
        long block_fill_size = data_end_offset - data_start_offset;

        // Consider all positions in this block
        while (record_i < count &&
            offsets[record_i] < current_block_end_offset) {
          // Map file position to element index in this tree assuming fixed
          // size records
          int element_offset_in_block = data_start_offset + (int)
              ((offsets[record_i] - current_block_start_offset) *
                  block_fill_size / current_block_size);
          
          int bol = RTree.skipToEOL(block_data, element_offset_in_block);
          elementId.set(current_block_start_offset + bol);
          int eol = RTree.skipToEOL(block_data, bol);
          // Skip empty line
          if (eol - bol < 2) {
            // Skip this line
            record_i++;
            continue;
          }
          line.set(block_data, bol, eol - bol);

          // Report this element to output
          if (output != null) {
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
    double ratio = cla.getSelectionRatio();
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
    if (size != 0) {
      record_count = sampleLocalWithSize(fs, inputFiles, size, output, stockObject);
    } else if (ratio != -1.0) {
      record_count = sampleMapReduceWithRatio(fs, inputFiles, ratio, output, stockObject);
    } else {
      if (count < BIG_SAMPLE) {
        record_count = sampleLocalByCount(fs, inputFiles, count, output, stockObject);
      } else {
        record_count = sampleLocalBig(fs, inputFiles, count, output, stockObject);
      }
    }
    System.out.println("Sampled "+record_count+" records");
  }
}
