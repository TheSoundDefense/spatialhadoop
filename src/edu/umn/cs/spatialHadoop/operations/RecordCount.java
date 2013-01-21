package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.Estimator;


/**
 * Calculates number of records in a file depending on its type. If the file
 * is a text file, it counts number of lines. If it's a grid file with no local
 * index, it counts number of non-empty lines. If it's a grid file with RTree
 * index, it counts total number of records stored in all RTrees.
 * @author eldawy
 *
 */
public class RecordCount {
  private static final ByteWritable ONEB = new ByteWritable((byte)1);
  private static final LongWritable ONEL = new LongWritable(1);

  public static class Map extends MapReduceBase implements
      Mapper<LongWritable, Text, ByteWritable, LongWritable> {
    public void map(LongWritable lineId, Text line,
        OutputCollector<ByteWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      output.collect(ONEB, ONEL);
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<ByteWritable, LongWritable, ByteWritable, LongWritable> {
    @Override
    public void reduce(ByteWritable id, Iterator<LongWritable> values,
        OutputCollector<ByteWritable, LongWritable> output, Reporter reporter)
            throws IOException {
      long total_lines = 0;
      while (values.hasNext()) {
        LongWritable next = values.next();
        total_lines += next.get();
      }
      output.collect(ONEB, new LongWritable(total_lines));
    }
  }
  
  /**
   * Counts the exact number of lines in a file by issuing a MapReduce job
   * that does the thing
   * @param conf
   * @param fs
   * @param file
   * @return
   * @throws IOException 
   */
  public static long recordCountMapReduce(FileSystem fs, Path file) throws IOException {
    JobConf job = new JobConf(RecordCount.class);
    
    Path outputPath = new Path(file.toUri().getPath()+".linecount");
    FileSystem outFs = outputPath.getFileSystem(job);
    outFs.delete(outputPath, true);
    
    job.setJobName("LineCount");
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);
    
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    job.setNumReduceTasks(1);
    
    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    
    TextInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    JobClient.runJob(job);
    
    // Read job result
    long lineCount = 0;
    FileStatus[] results = outFs.listStatus(outputPath);
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
        Text text = new Text();
        if (lineReader.readLine(text) > 0) {
          String str = text.toString();
          String[] parts = str.split("\t");
          lineCount = Long.parseLong(parts[1]);
        }
        lineReader.close();
      }
    }
    
    outFs.delete(outputPath, true);
    
    return lineCount;
  }
  
  /**
   * Counts the exact number of lines in a file by opening the file and
   * reading it line by line
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static long recordCountLocal(FileSystem fs, Path file) throws IOException {
    LineReader lineReader = new LineReader(fs.open(file));
    Text line = new Text();
    long lineCount = 0;
    
    while (lineReader.readLine(line) > 0) {
      if (line.getLength() > 0)
        lineCount++;
    }
    lineReader.close();
    return lineCount;
  }
  
  /**
   * Counts the approximate number of lines in a file by getting an approximate
   * average line length
   * @param fs
   * @param file
   * @return
   * @throws IOException
   */
  public static<T> long recordCountApprox(FileSystem fs, Path file) throws IOException {
    final long fileSize = fs.getFileStatus(file).getLen();
    final FSDataInputStream in = fs.open(file);
    
    Estimator<Long> lineEstimator = new Estimator<Long>(0.05);
    lineEstimator.setRandomSample(new Estimator.RandomSample() {
      
      @Override
      public double next() {
        int lineLength = 0;
        try {
          long randomFilePosition = (long)(Math.random() * fileSize);
          in.seek(randomFilePosition);
          
          // Skip the rest of this line
          byte lastReadByte;
          do {
            lastReadByte = in.readByte();
          } while (lastReadByte != '\n' && lastReadByte != '\r');

          while (in.getPos() < fileSize - 1) {
            lastReadByte = in.readByte();
            if (lastReadByte == '\n' || lastReadByte == '\r') {
              break;
            }
            lineLength++;
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        return lineLength+1;
      }
    });
    
    lineEstimator.setUserFunction(new Estimator.UserFunction<Long>() {
      @Override
      public Long calculate(double x) {
        return (long)(fileSize / x);
      }
    });
    
    lineEstimator.setQualityControl(new Estimator.QualityControl<Long>() {
      
      @Override
      public boolean isAcceptable(Long y1, Long y2) {
        return (double)Math.abs(y2 - y1) / Math.min(y1, y2) < 0.01;
      }
    });
    
    Estimator.Range<Long> lineCount = lineEstimator.getEstimate();
    in.close();
    
    return (lineCount.limit1 + lineCount.limit2) / 2;
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(RecordCount.class);
    Path inputFile = cla.getPath();
    FileSystem fs = inputFile.getFileSystem(conf);
    boolean local = cla.isLocal();
    boolean random = cla.isRandom();
    long lineCount;
    if (local) {
      if (random) {
        lineCount = recordCountApprox(fs, inputFile);
      } else {
        lineCount = recordCountLocal(fs, inputFile);
      }
    } else {
      lineCount = recordCountMapReduce(fs, inputFile);
    }
    System.out.println("Count of records in "+inputFile+" is "+lineCount);
  }

}
