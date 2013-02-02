package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.BinaryShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapReduce.BinarySpatialInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.PairWritable;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeArrayRecordReader;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 * @author eldawy
 *
 */
public class RedistributeJoin {
  private static final Log LOG = LogFactory.getLog(RedistributeJoin.class);

  public static class RedistributeJoinMap extends MapReduceBase
  implements Mapper<PairWritable<CellInfo>, PairWritable<? extends Writable>, Shape, Shape> {
    public void map(
        final PairWritable<CellInfo> key,
        final PairWritable<? extends Writable> value,
        final OutputCollector<Shape, Shape> output,
        Reporter reporter) throws IOException {
      int result_size;
      final Rectangle mapperMBR = key.first.getIntersection(key.second);
      
      if (value.first instanceof RTree) {
        @SuppressWarnings("unchecked")
        RTree<Shape> r1 = (RTree<Shape>) value.first;
        @SuppressWarnings("unchecked")
        RTree<Shape> r2 = (RTree<Shape>) value.second;
        result_size = RTree.spatialJoin(r1, r2,
            new RTree.ResultCollector2<Shape, Shape>() {
          @Override
          public void add(Shape x, Shape y) {
            Rectangle intersectionMBR = x.getMBR().getIntersection(y.getMBR());
            // Employ reference point duplicate avoidance technique 
            if (mapperMBR.contains(intersectionMBR.x, intersectionMBR.y)) {
              try {
                output.collect(x, y);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        });
      } else if (value.first instanceof ArrayWritable){
        // Join two arrays
        ArrayWritable ar1 = (ArrayWritable) value.first;
        ArrayWritable ar2 = (ArrayWritable) value.second;
        result_size = SpatialAlgorithms.SpatialJoin_planeSweep(
            (Shape[])ar1.get(), (Shape[])ar2.get(),
            new SpatialAlgorithms.ResultCollector2<Shape, Shape>() {
          @Override
          public void add(Shape x, Shape y) {
            Rectangle intersectionMBR = x.getMBR().getIntersection(y.getMBR());
            // Employ reference point duplicate avoidance technique 
            if (mapperMBR.contains(intersectionMBR.x, intersectionMBR.y)) {
              try {
                output.collect(x, y);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        });
      } else {
        throw new RuntimeException("Cannot join the values of type "+
            value.first.getClass());
      }
      LOG.info("Found: "+result_size+" pairs");
    }
  }
  
  /**
   * Reads a pair of arrays of shapes
   * @author eldawy
   *
   */
  public static class DJRecordReaderArray extends BinaryShapeRecordReader<CellInfo, ArrayWritable> {
    public DJRecordReaderArray(Configuration conf, CombineFileSplit fileSplits) throws IOException {
      super(conf, fileSplits);
    }
    
    @Override
    protected RecordReader<CellInfo, ArrayWritable> createRecordReader(
        Configuration conf, CombineFileSplit split, int i) throws IOException {
      FileSplit fsplit = new FileSplit(split.getPath(i),
          split.getStartOffsets()[i],
          split.getLength(i), split.getLocations());
      return new ShapeArrayRecordReader(conf, fsplit);
    }
  }

  /**
   * Input format that returns a record reader that reads a pair of arrays of
   * shapes
   * @author eldawy
   *
   */
  public static class DJInputFormatArray extends BinarySpatialInputFormat<CellInfo, ArrayWritable> {
    
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      InputSplit[] splits = super.getSplits(job, numSplits);
      System.out.println("Number of map tasks "+splits.length);
      return splits;
    }
    
    @Override
    public RecordReader<PairWritable<CellInfo>, PairWritable<ArrayWritable>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new DJRecordReaderArray(job, (CombineFileSplit)split);
    }
  }

  /**
   * Performs a redistribute join between the given files using the redistribute
   * join algorithm. Currently, we only support a pair of files.
   * @param fs
   * @param files
   * @param output
   * @return
   * @throws IOException 
   */
  public static <S extends Shape> long redistributeJoin(FileSystem fs, Path[] files,
      S stockShape,
      OutputCollector<S, S> output) throws IOException {
    JobConf job = new JobConf(RedistributeJoin.class);
    
    Path outputPath;
    FileSystem outFs = files[0].getFileSystem(job);
    do {
      outputPath = new Path("/"+files[0].getName()+
          ".dj_"+(int)(Math.random() * 1000000));
    } while (outFs.exists(outputPath));
    outFs.deleteOnExit(outputPath);
    
    job.setJobName("RedistributeJoin");
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setMapperClass(RedistributeJoinMap.class);
    job.setMapOutputKeyClass(stockShape.getClass());
    job.setMapOutputValueClass(stockShape.getClass());
    job.setBoolean(SpatialSite.AutoCombineSplits, true);
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
    job.setNumReduceTasks(0); // No reduce needed for this task

    job.setInputFormat(DJInputFormatArray.class);
    job.set(SpatialSite.SHAPE_CLASS, stockShape.getClass().getName());
    job.setOutputFormat(TextOutputFormat.class);
    
    String commaSeparatedFiles = "";
    for (int i = 0; i < files.length; i++) {
      if (i > 0)
        commaSeparatedFiles += ',';
      commaSeparatedFiles += files[i].toUri().toString();
    }
    DJInputFormatArray.addInputPaths(job, commaSeparatedFiles);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();

    S s1 = stockShape;
    @SuppressWarnings("unchecked")
	S s2 = (S) stockShape.clone();
    // Read job result
    FileStatus[] results = outFs.listStatus(outputPath);
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        if (output != null) {
          // Report every single result as a pair of shapes
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          while (lineReader.readLine(text) > 0) {
            String str = text.toString();
            String[] parts = str.split("\t", 2);
            s1.fromText(new Text(parts[0]));
            s2.fromText(new Text(parts[1]));
            output.collect(s1, s2);
          }
          lineReader.close();
        }
      }
    }
    
    outFs.delete(outputPath, true);
    
    return resultCount;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] inputPaths = cla.getPaths();
    JobConf conf = new JobConf(RedistributeJoin.class);
    FileSystem fs = inputPaths[0].getFileSystem(conf);
    Shape stockShape = cla.getShape(true);
    long t1 = System.currentTimeMillis();
    long resultSize = redistributeJoin(fs, inputPaths, stockShape, null);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }
}
