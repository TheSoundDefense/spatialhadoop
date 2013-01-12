package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TextSerializable;
import org.apache.hadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.TigerShape;
import edu.umn.cs.spatialHadoop.mapReduce.BlockFilter;
import edu.umn.cs.spatialHadoop.mapReduce.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeRecordReader;

/**
 * Performs k Nearest Neighbor (kNN) query over a spatial file.
 * @author eldawy
 *
 */
public class KNN {
  /**Logger for KNN*/
  private static final Log LOG = LogFactory.getLog(KNN.class);

  
  /**Configuration line name for query point*/
  public static final String QUERY_POINT =
      "edu.umn.cs.spatialHadoop.operations.KNN.QueryPoint";

  public static class PointWithK extends Point {
    /** SK, number of nearest neighbors required to find */
    public int k;

    public PointWithK() {
    }
    
    public PointWithK(Point point, int k) {
      super(point.x, point.y);
      this.k = k;
    }

    public PointWithK(long x, long y, int k) {
      super(x, y);
      this.k = k;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(k);
    }
    
    @Override
    public String toString() {
      return String.format("%s K=%x", super.toString(), k);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      this.k = in.readInt();
    }
    
    @Override
    public Text toText(Text text) {
      TextSerializerHelper.serializeLong(k, text, ',');
      return super.toText(text);
    }
    
    @Override
    public void fromText(Text text) {
      k = (int) TextSerializerHelper.consumeLong(text, ',');
      super.fromText(text);
    }
  }
  
  /**
   * A simple inner type that stores a shape with its distance
   * @author eldawy
   */
  public static abstract class ShapeWithDistance<S extends Shape>
      implements TextSerializable, Writable {
    public S shape;
    public long distance;
    
    public ShapeWithDistance(S shape, long distance) {
      this.shape = shape;
      this.distance = distance;
    }
    
    public abstract ShapeWithDistance<S> clone();
    
    @Override
    public Text toText(Text text) {
      TextSerializerHelper.serializeLong(distance, text, ';');
      return shape.toText(text);
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
  
  /*
   * Since the intermediate object should have a default constructor, we
   * provide subclasses with default constructors for every type of shape.
   */
  public static class PointWithDistance extends ShapeWithDistance<Point> {
    public PointWithDistance() {
      super(new Point(), 0);
    }
    
    public PointWithDistance(Point shape, long distance) {
      super(shape, distance);
    }

    @Override
    public ShapeWithDistance<Point> clone() {
      return new PointWithDistance(shape.clone(), distance);
    }
  }
  
  public static class RectangleWithDistance extends ShapeWithDistance<Rectangle> {
    public RectangleWithDistance() {
      super(new Rectangle(), 0);
    }

    public RectangleWithDistance(Rectangle shape, long distance) {
      super(shape, distance);
      // TODO Auto-generated constructor stub
    }
    
    @Override
    public ShapeWithDistance<Rectangle> clone() {
      return new RectangleWithDistance(shape.clone(), distance);
    }
  }
  
  public static class TigerShapeWithDistance extends ShapeWithDistance<TigerShape> {
    public TigerShapeWithDistance() {
      super(new TigerShape(), 0);
    }

    public TigerShapeWithDistance(TigerShape shape, long distance) {
      super(shape, distance);
    }
    
    @Override
    public ShapeWithDistance<TigerShape> clone() {
      return new TigerShapeWithDistance(shape.clone(), distance);
    }
  }
  
  public static class KNNFilter extends DefaultBlockFilter {
    /**User query*/
    private PointWithK queryPoint;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new PointWithK();
      queryPoint.fromText(new Text(job.get(QUERY_POINT)));
    }
    
    @Override
    public boolean processBlock(BlockLocation blk) {
      return (blk.getCellInfo() == null ||
          blk.getCellInfo().contains(queryPoint.x, queryPoint.y));
    }
  }
  
  /**
   * Mapper for KNN MapReduce. Calculates the distance between a shape and 
   * the query point.
   * @author eldawy
   *
   */
  public static class KNNMap<S extends Shape> extends MapReduceBase {
    /**A dummy intermediate value used instead of recreating it again and gain*/
    private static final ByteWritable ONE = new ByteWritable((byte)1);

    /**A temporary object to be used for output*/
    private ShapeWithDistance<S> temp;
    
    /**User query*/
    private PointWithK queryPoint;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new PointWithK();
      queryPoint.fromText(new Text(job.get(QUERY_POINT)));
      try {
        temp = job.getMapOutputValueClass().asSubclass(ShapeWithDistance.class)
            .newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    /**
     * Map for non-indexed (heap) blocks
     * @param id
     * @param shape
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(LongWritable id, S shape,
        OutputCollector<ByteWritable, ShapeWithDistance<S>> output,
        Reporter reporter) throws IOException {
      temp.shape = shape;
      temp.distance = (long)shape.distanceTo(queryPoint.x, queryPoint.y);
      output.collect(ONE, temp);
    }

    /**
     * Map for RTree indexed blocks
     * @param id
     * @param shapes
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(CellInfo cellInfo, RTree<S> shapes,
        final OutputCollector<ByteWritable, ShapeWithDistance<S>> output,
        Reporter reporter) throws IOException {
      shapes.knn(queryPoint.x, queryPoint.y, queryPoint.k, new RTree.ResultCollector2<S, Long>() {
        @Override
        public void add(S shape, Long distance) {
          try {
            temp.shape = shape;
            temp.distance = distance;
            output.collect(ONE, temp);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }
  
  public static class Map1<S extends Shape> extends KNNMap<S>
    implements Mapper<LongWritable, S, ByteWritable, ShapeWithDistance<S>> {}

  public static class Map2<S extends Shape> extends KNNMap<S>
    implements Mapper<CellInfo, RTree<S>, ByteWritable, ShapeWithDistance<S>> {}

  /**
   * Reduce (and combine) class for KNN MapReduce. Given a list of shapes,
   * choose the k with least distances.
   * @author eldawy
   *
   */
  public static class KNNReduce<S extends Shape> extends MapReduceBase implements
  Reducer<ByteWritable, ShapeWithDistance<S>, ByteWritable, ShapeWithDistance<S>> {
    /**A dummy intermediate value used instead of recreating it again and gain*/
    private static final ByteWritable ONE = new ByteWritable((byte)1);

    /**User query*/
    private PointWithK queryPoint;

    @Override
    public void configure(JobConf job) {
      super.configure(job);
      queryPoint = new PointWithK();
      queryPoint.fromText(new Text(job.get(QUERY_POINT)));
    }

    @Override
    public void reduce(ByteWritable dummy, Iterator<ShapeWithDistance<S>> values,
        OutputCollector<ByteWritable, ShapeWithDistance<S>> output, Reporter reporter)
            throws IOException {
      if (queryPoint.k == 0)
        return;
      ShapeWithDistance<S>[] knn = new ShapeWithDistance[queryPoint.k];
      int neighborsFound = 0;
      int maxi = 0;
      while (values.hasNext()) {
        ShapeWithDistance<S> s = values.next();
        if (neighborsFound < knn.length) {
          // Append to list if found less than required neighbors
          knn[neighborsFound] = s.clone();
          // Update point with maximum index if required
          if (s.distance > knn[maxi].distance)
            maxi = neighborsFound;
          // Increment total neighbors found
          neighborsFound++;
        } else {
          // Check if the new point is better than the farthest neighbor
          // Check if current point is better than the point with max distance
          if (s.distance < knn[maxi].distance)
            knn[maxi] = s.clone();

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
  public static<S extends Shape> long knnMapReduce(FileSystem fs, Path file,
      PointWithK queryPoint, S shape,
      OutputCollector<ByteWritable, ShapeWithDistance<S>> output)
      throws IOException {
    JobConf job = new JobConf(FileMBR.class);
    
    Path outputPath;
    FileSystem outFs = file.getFileSystem(job);
    do {
      outputPath = new Path(file.toUri().getPath()+
          ".knn_"+(int)(Math.random() * 1000000));
    } while (outFs.exists(outputPath));
    outFs.deleteOnExit(outputPath);
    
    Class shapeWithDistanceClass;
    if (shape instanceof Point) {
      shapeWithDistanceClass = PointWithDistance.class;
    } else if (shape instanceof Rectangle) {
      shapeWithDistanceClass = RectangleWithDistance.class;
    } else if (shape instanceof TigerShape) {
      shapeWithDistanceClass = TigerShapeWithDistance.class;
    } else {
      throw new RuntimeException("Class not supported "+shape.getClass());
    }
    
    job.setJobName("KNN");
    
    FSDataInputStream in = fs.open(file);
    if (in.readLong() == SpatialSite.RTreeFileMarker) {
      LOG.info("Performing KNN on RTree blocks");
      job.setMapperClass(Map2.class);
      job.setInputFormat(RTreeInputFormat.class);
    } else {
      LOG.info("Performing KNN on heap blocks");
      job.setMapperClass(Map1.class);
      // Combiner is needed for heap blocks
      job.setCombinerClass(KNNReduce.class);
      job.setInputFormat(ShapeInputFormat.class);
    }
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
    
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(shapeWithDistanceClass);
    Text text = new Text();
    queryPoint.toText(text);
    job.set(QUERY_POINT, text.toString());
    job.setClass(SpatialSite.FilterClass, KNNFilter.class, BlockFilter.class);
    
    job.setReducerClass(KNNReduce.class);
    job.setNumReduceTasks(1);
    
    job.set(SpatialSite.SHAPE_CLASS, shape.getClass().getName());
    String query_point_distance = queryPoint.x+","+queryPoint.y+","+0;
    job.setOutputFormat(TextOutputFormat.class);
    
    ShapeInputFormat.setInputPaths(job, file);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Submit the job
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();
    
    // Read job result
    if (output != null) {
      FileStatus[] results = outFs.listStatus(outputPath);
      try {
        for (FileStatus fileStatus : results) {
          if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
            // Report every single result as a pair of shape with distance
            ShapeWithDistance<S> shapeWithDistance =
                (ShapeWithDistance<S>) shapeWithDistanceClass.newInstance();
            shapeWithDistance.shape = shape;
            LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
            text.clear();
            while (lineReader.readLine(text) > 0) {
              String str = text.toString();
              String[] parts = str.split("\t", 2);
              shapeWithDistance.fromText(new Text(parts[1]));
              output.collect(null, shapeWithDistance);
            }
            lineReader.close();
          }
        }
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    
    outFs.delete(outputPath, true);
    
    return resultCount;
  }
  
  public static<S extends Shape> long knnLocal(FileSystem fs, Path file,
      PointWithK queryPoint, S shape,
      OutputCollector<ByteWritable, ShapeWithDistance<S>> output)
      throws IOException {
    long file_size = fs.getFileStatus(file).getLen();
    ShapeRecordReader<S> shapeReader =
        new ShapeRecordReader<S>(fs.open(file), 0, file_size);

    LongWritable key = shapeReader.createKey();
    
    ShapeWithDistance<S> stock;
    if (shape instanceof Point) {
      stock = (ShapeWithDistance<S>) new PointWithDistance();
    } else if (shape instanceof Rectangle) {
      stock = (ShapeWithDistance<S>) new RectangleWithDistance();
    } else if (shape instanceof TigerShape) {
      stock = (ShapeWithDistance<S>) new TigerShapeWithDistance();
    } else {
      throw new RuntimeException("Class not supported: "+shape.getClass());
    }
    ShapeWithDistance<S>[] knn = new ShapeWithDistance[queryPoint.k];

    while (shapeReader.next(key, shape)) {
      double distance = shape.distanceTo(queryPoint.x, queryPoint.y);
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
        knn[i] = stock.clone();
        knn[i].shape = shape;
        knn[i].distance = (long) distance;
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
    final Path inputFile = cla.getPath();
    Point queryPoint = cla.getPoint();
    System.out.println("Query: "+queryPoint);
    final FileSystem fs = inputFile.getFileSystem(conf);
    final int k = cla.getK();
    int count = cla.getCount();
    int concurrency = cla.getConcurrency();
    final Shape shape = cla.getShape(true);
    
    final Vector<Long> results = new Vector<Long>();
    
    if (queryPoint != null) {
      // User provided a query, use it
      long resultCount = 
          knnMapReduce(fs, inputFile, new PointWithK(queryPoint, k), shape, null);
      System.out.println("Found "+resultCount+" results");
    } else {
      // Generate query at random points
      final Vector<Thread> threads = new Vector<Thread>();
      final Vector<PointWithK> query_points = new Vector<PointWithK>();
      Sampler.sampleLocal(fs, inputFile, count, new OutputCollector<LongWritable, Shape>(){
        @Override
        public void collect(final LongWritable key, final Shape value) throws IOException {
          PointWithK query_point = new PointWithK();
          query_point.k = k;
          query_point.x = value.getMBR().x;
          query_point.y = value.getMBR().y;
          query_points.add(query_point);
          threads.add(new Thread() {
            @Override
            public void run() {
              try {
                PointWithK query_point =
                    query_points.elementAt(threads.indexOf(this));
                long result_count = knnMapReduce(fs, inputFile,
                        query_point, shape, null);
                results.add(result_count);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          });
        }
      }, shape);

      long t1 = System.currentTimeMillis();
      do {
        // Ensure that there is at least MaxConcurrentThreads running
        int i = 0;
        while (i < concurrency && i < threads.size()) {
          Thread.State state = threads.elementAt(i).getState(); 
          if (state == Thread.State.TERMINATED) {
            // Thread already terminated, remove from the queue
            threads.remove(i);
          } else if (state == Thread.State.NEW) {
            // Start the thread and move to next one
            threads.elementAt(i++).start();
          } else {
            // Thread is still running, skip over it
            i++;
          }
        }
        if (!threads.isEmpty()) {
          try {
            // Sleep for 10 seconds or until the first thread terminates
            threads.firstElement().join(10000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } while (!threads.isEmpty());
      long t2 = System.currentTimeMillis();
      System.out.println("Time for "+count+" jobs is "+(t2-t1)+" millis");
      System.out.println("Results: "+results);
    }
  }
}
