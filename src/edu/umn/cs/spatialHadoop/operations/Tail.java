package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text2;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import edu.umn.cs.CommandLineArguments;

/**
 * Reads the last n lines of a text file
 * @author eldawy
 *
 */
public class Tail {

  public static<T extends TextSerializable> int tail(FileSystem fs, Path file,
      int n, T stockObject, OutputCollector<LongWritable, T> output)
          throws IOException {
    // Read the last line
    Text last_line = new Text();
    FSDataInputStream result_stream = fs.open(file);
    long last_read_byte = fs.getFileStatus(file).getLen();
    byte[] buffer = new byte[4096];
    long last_eol_offset = last_read_byte;
    LongWritable line_offset = new LongWritable();
    
    int num_of_lines_read = 0;
    do {
      long first_byte_to_read =
          (last_read_byte - 1) - (last_read_byte - 1) % buffer.length;
      result_stream.seek(first_byte_to_read);
      int bytes_to_read = (int) (last_read_byte - first_byte_to_read);
      result_stream.read(buffer, 0, bytes_to_read);
      
      int start_of_last_line = bytes_to_read;
      while (start_of_last_line > 0 && num_of_lines_read < n) {
        start_of_last_line--;
        if (buffer[start_of_last_line] == '\n'
            || buffer[start_of_last_line] == '\r') {
          long offset_of_this_line = start_of_last_line + first_byte_to_read;
          if (last_eol_offset - offset_of_this_line > 1) {

            if (output != null) {
              Text this_line = new Text();
              this_line.append(buffer, (start_of_last_line+1), bytes_to_read - (start_of_last_line+1));
              this_line.append(last_line.getBytes(), 0, last_line.getLength());
              // TODO report temp_text
              line_offset.set(offset_of_this_line);
              stockObject.fromText(this_line);
              output.collect(line_offset, stockObject);
              last_line.clear();
            }

            num_of_lines_read++;
          }
          last_eol_offset = offset_of_this_line;
          bytes_to_read = start_of_last_line;
        }
      }
      
      // Prepend read bytes to last_line
      Text temp_text = new Text();
      temp_text.append(buffer, start_of_last_line, bytes_to_read - start_of_last_line);
      temp_text.append(last_line.getBytes(), 0, last_line.getLength());
      last_line = temp_text;
      last_read_byte = first_byte_to_read;
    } while (num_of_lines_read < n && last_read_byte > 0);
    result_stream.close();

    if (num_of_lines_read < n) {
      line_offset.set(0);
      stockObject.fromText(last_line);
      output.collect(line_offset, stockObject);
      num_of_lines_read++;
    }
    
    return num_of_lines_read;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(Sampler.class);
    Path inputFile = cla.getPath();
    FileSystem fs = inputFile.getFileSystem(conf);
    int count = cla.getCount();
    TextSerializable stockObject = cla.getShape(true);
    if (stockObject == null)
      stockObject = new Text2();

    tail(fs, inputFile, count, stockObject, new OutputCollector<LongWritable, TextSerializable>() {

      @Override
      public void collect(LongWritable key, TextSerializable value)
          throws IOException {
        System.out.println(key+": " +value);
      }
    });
  }
}
