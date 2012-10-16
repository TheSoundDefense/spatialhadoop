package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.PointWithK;
import edu.umn.cs.spatialHadoop.mapReduce.STextOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapReduce.SplitCalculator;

/**
 * Performs k Nearest Neighbor (kNN) query over a spatial file.
 * @author eldawy
 *
 */
public class KNN {
  /**Configration line name for query point*/
  public static final String QUERY_POINT =
      "edu.umn.cs.spatialHadoop.operations.KNN.QueryPoint";

  /**
   * A simple inner type that stores a shape with its distance
   * @author eldawy
   */
  public static class ShapeWithDistance implements TextSerializable, Writable {
    public Shape shape;
    public long distance;
    
    public ShapeWithDistance() {
      this.shape = new TigerShape();
    }
    
    public ShapeWithDistance(Shape shape, long distance) {
      this.shape = shape;
      this.distance = distance;
    }
    
    @Override
    public ShapeWithDistance clone() {
      return new ShapeWithDistance(shape, distance);
    }

    @Override
    public void toText(Text text) {
      TextSerializerHelper.serializeLong(distance, text, ';');
      shape.toText(text);
    }

    @Override
    public void fromText(Text text) {
      byte[] buf = text.getBytes();
      int separator = 0;
      while (buf[separator] != ';')
        separator++;
      distance = TextSerializerHelper.deserializeLong(buf, 0, separator++);
      text.set(buf, separator, text.getLength() - separator);
      shape.fromText(text);
    }

    @Override
    public String writeToString() {
      Text text = new Text();
      toText(text);
      return text.toString();
    }

    @Override
    public void readFromString(String s) {
      Text text = new Text(s);
      this.fromText(text);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(distance);
      shape.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.distance = in.readLong();
      shape.readFields(in);
    }
  }
  
  /**
   * Mapper for KNN MapReduce. Calculates the distance between a shape and 
   * the query point.
   * @author eldawy
   *
   */
  public static class Map extends MapReduceBase
  implements
      Mapper<LongWritable, Shape, ByteWritable, ShapeWithDistance> {
    /**A dummy intermediate value used instead of recreating it again and gain*/
    private static final ByteWritable ONE = new ByteWritable((byte)1);

    /**A temporary object to be used for output*/
    private final ShapeWithDistance temp = new ShapeWithDistance();
    
    /**User query*/
    private PointWithK queryPoint;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new PointWithK();
      queryPoint.readFromString(job.get(QUERY_POINT));
    }

    public void map(LongWritable id, Shape shape,
        OutputCollector<ByteWritable, ShapeWithDistance> output,
        Reporter reporter) throws IOException {
      temp.shape = shape;
      temp.distance = (long)shape.getAvgDistanceTo(queryPoint);
      output.collect(ONE, temp);
    }
  }

  /**
   * Reduce (and combine) class for KNN MapReduce. Given a list of shapes,
   * choose the k with least distances.
   * @author eldawy
   *
   */
  public static class Reduce extends MapReduceBase implements
  Reducer<ByteWritable, ShapeWithDistance, ByteWritable, ShapeWithDistance> {
    /**A dummy intermediate value used instead of recreating it again and gain*/
    private static final ByteWritable ONE = new ByteWritable((byte)1);

    /**User query*/
    private PointWithK queryPoint;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new PointWithK();
      queryPoint.readFromString(job.get(QUERY_POINT));
    }

    @Override
    public void reduce(ByteWritable dummy, Iterator<ShapeWithDistance> values,
        OutputCollector<ByteWritable, ShapeWithDistance> output, Reporter reporter)
            throws IOException {
      ShapeWithDistance[] knn = new ShapeWithDistance[queryPoint.k];
      int neighborsFound = 0;
      int maxi = 0;
      while (values.hasNext()) {
        ShapeWithDistance s = values.next();
        if (neighborsFound < knn.length) {
          // Append to list if found less than required neighbors
          knn[neighborsFound] = (ShapeWithDistance) s.clone();
          // Update point with maximum index if required
          if (s.distance > knn[maxi].distance)
            maxi = neighborsFound;
          // Increment total neighbors found
          neighborsFound++;
        } else {
          // Check if the new point is better than the farthest neighbor
          // Check if current point is better than the point with max distance
          if (s.distance < knn[maxi].distance)
            knn[maxi] = (ShapeWithDistance) s.clone();

          // Update point with maximum index
          for (int i = 0; i < knn.length;i++) {
            if (knn[i].distance > knn[maxi].distance)
              maxi = i;
          }
        }
      }

      for (int i = 0; i < neighborsFound; i++) {
        output.collect(ONE, knn[i]);
      }
    }
  }
  
  /**
   * A MapReduce version of KNN query.
   * @param fs
   * @param file
   * @param queryPoint
   * @param shape
   * @param output
   * @return
   * @throws IOException
   */
  public static long knnMapReduce(FileSystem fs, Path file,
      PointWithK queryPoint, Shape shape,
      OutputCollector<ByteWritable, ShapeWithDistance> output)
      throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath = new Path(file.toUri().getPath()+".knn");
    FileSystem outFs = outputPath.getFileSystem(job);
    outFs.delete(outputPath, true);
    
    job.setJobName("KNN");
    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(ShapeWithDistance.class);
    job.set(QUERY_POINT, queryPoint.writeToString());
    
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);
    
    job.setInputFormat(ShapeInputFormat.class);
    job.set(ShapeRecordReader.SHAPE_CLASS, TigerShape.class.getName());
    String query_point_distance = queryPoint.x+","+queryPoint.y+","+0;
    job.set(SplitCalculator.QUERY_POINT_DISTANCE, query_point_distance);
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
          // Report every single result as a pair of shape with distance
          ShapeWithDistance shapeWithDistance = new ShapeWithDistance();
          shapeWithDistance.shape = shape;
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          if (lineReader.readLine(text) > 0) {
            String str = text.toString();
            String[] parts = str.split("\t", 2);
            shapeWithDistance.readFromString(parts[1]);
            output.collect(null, shapeWithDistance);
          }
          lineReader.close();
        }
      }
    }
    
    outFs.delete(outputPath, true);
    
    return resultCount;
  }
  
  public static long knnLocal(FileSystem fs, Path file,
      PointWithK queryPoint, Shape shape,
      OutputCollector<ByteWritable, ShapeWithDistance> output)
      throws IOException {
    long file_size = fs.getFileStatus(file).getLen();
    ShapeRecordReader shapeReader =
        new ShapeRecordReader(fs.open(file), 0, file_size);

    LongWritable key = shapeReader.createKey();
    
    ShapeWithDistance[] knn = new ShapeWithDistance[queryPoint.k];

    while (shapeReader.next(key, shape)) {
      double distance = shape.getAvgDistanceTo(queryPoint);
      int i = queryPoint.k - 1;
      while (i >= 0 && (knn[i] == null || knn[i].distance > distance)) {
        i--;
      }
      i++;
      if (i < queryPoint.k) {
        if (knn[i] != null) {
          for (int j = queryPoint.k - 1; j > i; j--)
            knn[j] = knn[j-1];
        }
        knn[i] = new ShapeWithDistance(shape, (long)distance);
      }
    }
    shapeReader.close();
    
    long resultCount = 0;
    for (int i = 0; i < knn.length; i++) {
      if (knn[i] != null) {
        if (output != null) {
          output.collect(null, knn[i]);
        }
        resultCount++;
      }
    }
    return resultCount;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(FileMBR.class);
    Path inputFile = cla.getFilePath();
    PointWithK queryPoint = cla.getPointWithK();
    System.out.println("Query: "+queryPoint);
    FileSystem fs = inputFile.getFileSystem(conf);
    long resultCount = 
        knnMapReduce(fs, inputFile, queryPoint, new TigerShape(), null);
    System.out.println("Found "+resultCount+" results");
  }

}
