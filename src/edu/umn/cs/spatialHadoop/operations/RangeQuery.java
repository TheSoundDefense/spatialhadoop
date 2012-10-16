package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.STextOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapReduce.SplitCalculator;

/**
 * Performs a range query over a spatial file.
 * @author eldawy
 *
 */
public class RangeQuery {
  /**Logger for RangeQuery*/
  private static final Log LOG = LogFactory.getLog(RangeQuery.class);
  
  /**Name of the config line that stores the class name of the query shape*/
  public static final String QUERY_SHAPE_CLASS =
      "edu.umn.cs.spatialHadoop.operations.RangeQuery.QueryShapeClass";

  /**Name of the config line that stores the query shape*/
  public static final String QUERY_SHAPE =
      "edu.umn.cs.spatialHadoop.operations.RangeQuery.QueryShape";
  
  /**
   * The map function used for range query
   * @author eldawy
   *
   * @param <T>
   */
  public static class Map<T extends Shape> extends MapReduceBase {
    /**A shape that is used to filter input*/
    private Shape queryShape;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
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
    }
    
    private static final ByteWritable ONEB = new ByteWritable((byte)1);
    
    /**
     * Map function for non-indexed blocks
     */
    public void map(LongWritable shapeId, T shape,
        OutputCollector<ByteWritable, T> output, Reporter reporter)
            throws IOException {
      if (shape.isIntersected(queryShape)) {
        output.collect(ONEB, shape);
      }
    }
    
    /**
     * Map function for RTree indexed blocks
     * @param shapeId
     * @param shapes
     * @param output
     * @param reporter
     */
    public void map(CellInfo cellInfo, RTree<T> shapes,
        final OutputCollector<ByteWritable, T> output, Reporter reporter) {
      LOG.info("Searching in the range: "+cellInfo+" for the query: "+queryShape.getMBR());
      int count = shapes.search(queryShape.getMBR(), new RTree.ResultCollector<T>() {
        @Override
        public void add(T x) {
          try {
            output.collect(ONEB, x);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
      LOG.info("count: "+count);
    }
  }
  
  /** Mapper for non-indexed blocks */
  public static class Map1<T extends Shape> extends Map<T> implements
      Mapper<LongWritable, T, ByteWritable, T> { }

  /** Mapper for RTree indexed blocks */
  public static class Map2<T extends Shape> extends Map<T> implements
      Mapper<CellInfo, RTree<T>, ByteWritable, T> { }
  
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
    job.set(QUERY_SHAPE_CLASS, queryShape.getClass().getName());
    job.set(QUERY_SHAPE, queryShape.writeToString());
    job.setNumReduceTasks(0);

    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(shape.getClass());
    job.set(SplitCalculator.QUERY_RANGE, queryShape.getMBR().writeToString());
    // Decide which map function to use depending on how blocks are indexed
    // And also which input format to use
    FSDataInputStream in = fs.open(file);
    if (in.readLong() == RTreeGridRecordWriter.RTreeFileMarker) {
      // RTree indexed file
      LOG.info("Searching an RTree indexed file");
      job.setMapperClass(Map2.class);
      job.setInputFormat(RTreeInputFormat.class);
    } else {
      // A file with no local index
      LOG.info("Searching a non local-indexed file");
      job.setMapperClass(Map1.class);
      job.setInputFormat(ShapeInputFormat.class);
    }
    job.set(ShapeRecordReader.SHAPE_CLASS, TigerShape.class.getName());
    in.close();
    
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
        resultCount = RecordCount.recordCountLocal(outFs, fileStatus.getPath());
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
