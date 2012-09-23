package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.PointWithK;
import edu.umn.cs.spatialHadoop.TigerShapeWithDistance;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class KNNMapReduce {
  public static final Log LOG = LogFactory.getLog(KNNMapReduce.class);
  
  public static final String QUERY_POINT = "edu.umn.cs.spatial.mapReduce.RQMapReduce.QueryPoint";
  public static PointWithK queryPoint;

  public static class Map extends MapReduceBase
  implements
      Mapper<LongWritable, TigerShapeWithDistance, LongWritable, TigerShapeWithDistance> {
    private static final LongWritable ONE = new LongWritable(1);

    public void map(LongWritable id, TigerShapeWithDistance shape,
        OutputCollector<LongWritable, TigerShapeWithDistance> output,
        Reporter reporter) throws IOException {
      shape.distance = shape.getAvgDistanceTo(queryPoint);
      output.collect(ONE, shape);
    }
  }
	
  public static class Reduce extends MapReduceBase implements
      Reducer<LongWritable, TigerShapeWithDistance, LongWritable, TigerShapeWithDistance> {
    @Override
    public void reduce(LongWritable id, Iterator<TigerShapeWithDistance> values,
        OutputCollector<LongWritable, TigerShapeWithDistance> output, Reporter reporter)
        throws IOException {
      TigerShapeWithDistance[] knn = new TigerShapeWithDistance[queryPoint.k];
      int neighborsFound = 0;
      int maxi = 0;
      while (values.hasNext()) {
        TigerShapeWithDistance s = values.next();
        if (neighborsFound < knn.length) {
          // Append to list if found less than required neighbors
          knn[neighborsFound] = (TigerShapeWithDistance) s.clone();
          // Update point with maximum index if required
          if (s.distance > knn[maxi].distance)
            maxi = neighborsFound;
          // Increment total neighbors found
          neighborsFound++;
        } else {
          // Check if the new point is better than the farthest neighbor
          
          // Check if current point is better than the point with max distance
          if (s.distance < knn[maxi].distance)
            knn[maxi] = (TigerShapeWithDistance) s.clone();
          
          // Update point with maximum index
          for (int i = 0; i < knn.length;i++) {
            if (knn[i].distance > knn[maxi].distance)
              maxi = i;
          }
        }
      }
      
      for (int i = 0; i < neighborsFound; i++) {
        output.collect(id, knn[i]);
      }
    }

  }

  public static void localKnn(JobConf conf, Path inputFile, Path outputFile, PointWithK queryPoint) throws IOException {
    Random rand = new Random();
    TigerShapeWithDistance[] knn = new TigerShapeWithDistance[queryPoint.k];
    FileSystem fs = FileSystem.getLocal(conf);
    long fileLength = fs.getFileStatus(inputFile).getLen();
    PrintStream output = new PrintStream(fs.create(outputFile));
    TigerShapeRecordReader reader = new TigerShapeRecordReader(conf, fs.open(inputFile), 0, fileLength);
    LongWritable key = reader.createKey();
    TigerShape value = reader.createValue();
    while (reader.next(key, value)) {
      double distance = value.getAvgDistanceTo(queryPoint);
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
        knn[i] = new TigerShapeWithDistance(value, distance);
      }
    }
    output.close();
  }
	
	/**
	 * Entry point to the file.
	 * Params point:<query point> <input filenames> <output filename>
	 * query rectangle: in the form x1,y1,x2,y2
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	  CommandLineArguments cla = new CommandLineArguments(args);
	  JobConf conf = new JobConf(KNNMapReduce.class);
	  
    if (!FileSystem.get(conf).exists(cla.getInputPath()) &&
        FileSystem.getLocal(conf).exists(cla.getInputPath())) {
      conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
      // Run on a local file. No MapReduce
      localKnn(conf, cla.getInputPath(), cla.getOutputPath(), cla.getPointWithK());
     return;
    }

    conf.setJobName("KNN");
    
    // Retrieve query point and store it in the job
    TigerShapeWithDistance queryPoint = new TigerShapeWithDistance(0, cla.getPointWithK(), 0.0);
    System.out.println("PointWithK: "+queryPoint);
    conf.set(QUERY_POINT, ((PointWithK)queryPoint.shape).writeToString());
    System.out.println("PointWithK in conf: "+conf.get(QUERY_POINT));
    
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(TigerShapeWithDistance.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    conf.setCombinerClass(Reduce.class);

    conf.setInputFormat(KNNInputFormat.class);
    conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShapeWithDistance.class.getName());
    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Point.class.getName());
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputCommitter(KNNOutputCommitter.class);

    // All files except first and last one are input files
    Path inputPath = cla.getInputPath();
    RQInputFormat.setInputPaths(conf, inputPath);
    
    boolean jobFinished = false;

    // Get grid info of the file to be processed
    FileSystem fileSystem = FileSystem.get(conf);
    
    GridInfo gridInfo = fileSystem.getFileStatus(inputPath).getGridInfo();

    if (gridInfo == null) {
      // Processing a heap file
      Path outputPath = cla.getOutputPath();
      FileOutputFormat.setOutputPath(conf, outputPath);
      // Heap file is processed in one pass
      JobClient.runJob(conf);
      return;
    }

    int round = 0;

    Vector<BlockLocation> processedBlocks = new Vector<BlockLocation>();
    SplitCalculator.KNNBlocksInRange(fileSystem, inputPath, queryPoint, processedBlocks);
    
    while (!jobFinished) {
      conf.set(SplitCalculator.QUERY_POINT_DISTANCE,
          ((Point)queryPoint.shape).x+","+((Point)queryPoint.shape).y+
          ","+(long)queryPoint.distance);
      // Last argument is the base name output file
      Path outputPath = new Path(args[args.length - 1]+"_"+round);
      // Delete output path if existing
      fileSystem.delete(outputPath, true);
      FileOutputFormat.setOutputPath(conf, outputPath);

      JobClient.runJob(conf);

      // Check that results are correct
      FileStatus[] resultFiles = fileSystem.listStatus(outputPath);
      // Maximum distance of neighbors
      double farthestNeighbor = 0.0;
      for (FileStatus resultFile : resultFiles) {
        if (resultFile.getLen() > 0) {
          BufferedReader in = new BufferedReader(new InputStreamReader(fileSystem.open(resultFile.getPath())));
          String line;
          while ((line = in.readLine()) != null) {
            String pattern = "distance: ";
            // search for 'distance: '
            int i = line.indexOf(pattern);
            // Parse the rest of the line to get the distance
            double distance = Double.parseDouble(line.substring(i + pattern.length()));
            if (distance > farthestNeighbor)
              farthestNeighbor = distance;
          }
          in.close();
        }
      }
      queryPoint.distance = farthestNeighbor;

      // Ensure that we don't need to process more blocks to find the final answer
      Vector<BlockLocation> processedBlocksForNextRound = new Vector<BlockLocation>();
      SplitCalculator.KNNBlocksInRange(fileSystem, inputPath, queryPoint, processedBlocksForNextRound);
      jobFinished = processedBlocks.size() == processedBlocksForNextRound.size();
      processedBlocks = processedBlocksForNextRound;
      ++round;
    }
	}
}
