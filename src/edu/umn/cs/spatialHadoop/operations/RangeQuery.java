package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.STextOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.SplitCalculator;

/**
 * Performs a range query over a spatial file.
 * @author eldawy
 *
 */
public class RangeQuery {
  
  /**Name of the config line that stores the class name of the query shape*/
  public static final String QUERY_SHAPE_CLASS =
      "edu.umn.cs.spatialHadoop.operations.RangeQuery.QueryShapeClass";

  /**Name of the config line that stores the query shape*/
  public static final String QUERY_SHAPE =
      "edu.umn.cs.spatialHadoop.operations.RangeQuery.QueryShape";
  
  /**A shape that is used to filter input*/
  public static Shape queryShape;
  
  public static class Map<T extends Shape> extends MapReduceBase implements
  Mapper<LongWritable, T, ByteWritable, T> {
    private static final ByteWritable ONEB = new ByteWritable((byte)1);
    public void map(LongWritable shapeId, T shape,
        OutputCollector<ByteWritable, T> output, Reporter reporter)
            throws IOException {
      if (shape.isIntersected(queryShape)) {
        output.collect(ONEB, shape);
      }
    }
  }
  
  /**
   * An input format that sets the query range before starting the mapper 
   * @author eldawy
   */
  public static class InputFormat extends ShapeInputFormat {
    @Override
    public RecordReader<LongWritable, Shape> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      try {
        String queryShapeClassName = job.get(QUERY_SHAPE_CLASS);
        Class<? extends Shape> queryShapeClass =
            Class.forName(queryShapeClassName).asSubclass(Shape.class);
        queryShape = queryShapeClass.newInstance();
        queryShape.readFromString(job.get(QUERY_SHAPE));
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }

      return super.getRecordReader(split, job, reporter);
    }
  }
  
  /**
   * Performs a range query using MapReduce
   * @param fs
   * @param file
   * @param queryRange
   * @param shape
   * @param output
   * @return
   * @throws IOException
   */
  public static long rangeQueryMapReduce(FileSystem fs, Path file,
      Shape queryShape, Shape shape, OutputCollector<LongWritable, Shape> output)
      throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath = new Path(file.toUri().getPath()+".rangequery");
    FileSystem outFs = outputPath.getFileSystem(job);
    outFs.delete(outputPath, true);
    
    job.setJobName("RangeQuery");
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.set(QUERY_SHAPE_CLASS, queryShape.getClass().getName());
    job.set(QUERY_SHAPE, queryShape.writeToString());
    job.set(SplitCalculator.QUERY_RANGE, queryShape.getMBR().writeToString());
    
    job.setMapperClass(Map.class);
    
    job.setInputFormat(InputFormat.class);
    job.set(ShapeRecordReader.SHAPE_CLASS, TigerShape.class.getName());
    job.setOutputFormat(STextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    STextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    JobClient.runJob(job);
    
    // Read job result
    FileStatus[] results = outFs.listStatus(outputPath);
    long resultCount = 0;
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        resultCount = LineCount.lineCountLocal(outFs, fileStatus.getPath());
        if (output != null) {
          // Report every single result
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          if (lineReader.readLine(text) > 0) {
            String str = text.toString();
            String[] parts = str.split("\t", 2);
            shape.readFromString(parts[1]);
            output.collect(null, shape);
          }
          lineReader.close();
        }
      }
    }
    
    outFs.delete(outputPath, true);
    
    return resultCount;
  }
  
  /**
   * Runs a range query on the local machine by iterating over the whole file.
   * @param fs - FileSystem that contains input file
   * @param file - path to the input file
   * @param queryRange - The range to look in
   * @param shape - An instance of the shape stored in file
   * @param output - Output is sent to this collector. If <code>null</code>,
   *  output is not collected and only the number of results is returned. 
   * @return number of results found
   * @throws IOException
   */
  public static long rangeQueryLocal(FileSystem fs, Path file,
      Shape queryRange, Shape shape, OutputCollector<LongWritable, Shape> output)
      throws IOException {
    long file_size = fs.getFileStatus(file).getLen();
    ShapeRecordReader shapeReader =
        new ShapeRecordReader(fs.open(file), 0, file_size);

    long resultCount = 0;
    LongWritable key = shapeReader.createKey();

    while (shapeReader.next(key, shape)) {
      if (shape.isIntersected(queryRange)) {
        resultCount++;
        if (output != null) {
          output.collect(key, shape);
        }
      }
    }
    shapeReader.close();
    return resultCount;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(FileMBR.class);
    Path inputFile = cla.getFilePath();
    Rectangle queryRange = cla.getRectangle();
    FileSystem fs = inputFile.getFileSystem(conf);
    long resultCount = 
        rangeQueryMapReduce(fs, inputFile, queryRange, new TigerShape(), null);
    System.out.println("Found "+resultCount+" results");
  }
}
