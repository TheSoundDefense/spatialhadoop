package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

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
    
    public void map(
        LongWritable id,
        TigerShapeWithDistance shape,
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

	
	/**
	 * Entry point to the file.
	 * Params <query rectangle> <input filenames> <output filename>
	 * query rectangle: in the form x1,y1,x2,y2
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(KNNMapReduce.class);
      conf.setJobName("KNN");
      
      // Retrieve query point and store it in the job
      String queryPointStr = args[0];
      conf.set(QUERY_POINT, queryPointStr);
      
      conf.setOutputKeyClass(LongWritable.class);
      conf.setOutputValueClass(TigerShapeWithDistance.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);
      conf.setCombinerClass(Reduce.class);

      conf.setInputFormat(KNNInputFormat.class);
      conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShapeWithDistance.class.getName());
      conf.set(TigerShapeRecordReader.SHAPE_CLASS, Point.class.getName());
      conf.setOutputFormat(TextOutputFormat.class);

      // All files except first and last one are input files
      Path[] inputPaths = new Path[args.length - 2];
      for (int i = 1; i < args.length - 1; i++)
        RQInputFormat.addInputPath(conf, inputPaths[i-1] = new Path(args[i]));
      
      boolean jobFinished = false;

      // Get grid info of the file to be processed
      FileSystem fileSystem = FileSystem.get(conf);
      GridInfo gridInfo = fileSystem.getFileStatus(inputPaths[0]).getGridInfo();

      if (gridInfo == null) {
        Path outputPath = new Path(args[args.length - 1]);
        FileOutputFormat.setOutputPath(conf, outputPath);
        // Heap file is processed in one pass
        JobClient.runJob(conf);
        return;
      }

      String[] parts = queryPointStr.split(",");
      Point queryPoint = new Point(Long.parseLong(parts[0]), Long.parseLong(parts[1]));

      // Start with a rectangle that contains the query point
      Rectangle processedArea = new Rectangle(queryPoint.x, queryPoint.y, 1, 1);
      
      int round = 0;

      // Retrieve all blocks to be able to select blocks to be processed
      BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(inputPaths[0], 0, fileSystem.getFileStatus(inputPaths[0]).getLen());
      
      while (!jobFinished) {
      conf.set(SplitCalculator.QUERY_RANGE, processedArea.writeToString());
        // Last argument is the base name output file
        Path outputPath = new Path(args[args.length - 1]+"_"+round);
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

        jobFinished = true;

        // Ensure that maximum distance cannot go outside current cell
        for (int i = 0; i < inputPaths.length; i++) {
          // Find cell that contains query point; the one that was actually processed
          double minDistance = processedArea.getMinDistanceTo(queryPoint);
          if (minDistance < farthestNeighbor) {
            // TODO ensure that there is another grid cell at that distance
            // This indicates that there might be a nearer neighbor in
            // an adjacent cell

            // Add all grid cells that need to be processed
            for (BlockLocation blockLocation : blockLocations) {
              Rectangle rect = blockLocation.getCellInfo();
              if (rect.getMinDistanceTo(queryPoint) < farthestNeighbor &&
                  !processedArea.union(rect).equals(processedArea)) {
                processedArea = (Rectangle) processedArea.union(rect);
                jobFinished = false;
              }
            }
          }
        }
        ++round;

      }
	}
}
