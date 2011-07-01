package edu.umn.cs.spatial.mapReduce;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.util.LineReader;

import edu.umn.edu.spatial.Point;
import edu.umn.edu.spatial.PointWithDistance;
import edu.umn.edu.spatial.PointWithK;
import edu.umn.edu.spatial.Rectangle;


/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class KNNMapReduce {
  public static final Log LOG = LogFactory.getLog(KNNMapReduce.class);

  public static class Map extends MapReduceBase
  implements
  Mapper<PointWithK, Point, PointWithK, PointWithDistance> {
    public void map(
        PointWithK queryPoint,
        Point inputPoint,
        OutputCollector<PointWithK, PointWithDistance> output,
        Reporter reporter) throws IOException {
      output.collect(queryPoint, new PointWithDistance(inputPoint, inputPoint.distanceTo(queryPoint)));
    }
  }
	
  public static class Reduce extends MapReduceBase implements
      Reducer<PointWithK, PointWithDistance, Point, PointWithDistance> {
    @Override
    public void reduce(PointWithK key, Iterator<PointWithDistance> values,
        OutputCollector<Point, PointWithDistance> output, Reporter reporter)
        throws IOException {
      PointWithDistance[] knn = new PointWithDistance[key.k];
      int neighborsFound = 0;
      int maxi = 0;
      while (values.hasNext()) {
        PointWithDistance p = values.next();
        if (neighborsFound < knn.length) {
          // Append to list if found less than required neighbors
          knn[neighborsFound] = (PointWithDistance) p.clone();
          // Update point with maximum index if required
          if (p.getDistance() > knn[maxi].getDistance())
            maxi = neighborsFound;
          // Increment total neighbors found
          neighborsFound++;
        } else {
          // Check if the new point is better that the farthest neighbor
          
          // Check if current point is better than the point with max distance
          if (p.getDistance() < knn[maxi].getDistance())
            knn[maxi] = (PointWithDistance) p.clone();
          
          // Update point with maximum index
          for (int i = 0; i < knn.length;i++) {
            if (knn[i].getDistance() > knn[maxi].getDistance())
              maxi = i;
          }
        }
      }
      
      for (int i = 0; i < neighborsFound; i++) {
        output.collect(key, knn[i]);
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
      
      // Retrieve query rectangle and store it to an HDFS file
      PointWithK queryPoint = new PointWithK();
      String[] parts = args[0].split(",");
      
      queryPoint.x = Integer.parseInt(parts[0]);
      queryPoint.y = Integer.parseInt(parts[1]);
      queryPoint.k = Integer.parseInt(parts[2]);
      
      // Get the HDFS file system
      FileSystem fs = FileSystem.get(conf);
      Path queryFilepath = new Path("/knn_query");
      
      // Open an output stream for the file
      FSDataOutputStream out = fs.create(queryFilepath, true);
      PrintStream ps = new PrintStream(out);
      ps.print(0+","+queryPoint.x +","+ queryPoint.y +","+
          queryPoint.k);
      ps.close();
      
      // add this query file as the first input path to the job
      RQInputFormat.addInputPath(conf, queryFilepath);
      
      conf.setOutputKeyClass(PointWithK.class);
      conf.setOutputValueClass(PointWithDistance.class);

      conf.setMapperClass(Map.class);
      conf.setReducerClass(Reduce.class);
      conf.setCombinerClass(Reduce.class);

      conf.setInputFormat(KNNInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      // All files except first and last ones are input files
      Path[] inputPaths = new Path[args.length - 2];
      for (int i = 1; i < args.length - 1; i++)
        RQInputFormat.addInputPath(conf, inputPaths[i-1] = new Path(args[i]));
      
      boolean jobFinished = false;

      // Get grid info of the file to be processed
      GridInfo gridInfo = fs.getFileStatus(inputPaths[0]).getGridInfo();

      if (gridInfo == null) {
        JobClient.runJob(conf);
	return;
      }
      
      // Calculate initial rectangle to be processed (the one that contains the query point)
      int column = (int) ((queryPoint.x - gridInfo.xOrigin) / gridInfo.cellWidth);
      int row = (int) ((queryPoint.y - gridInfo.yOrigin) / gridInfo.cellHeight);
      Rectangle rectProcessed = new Rectangle(
          0,
          (int)(column * gridInfo.cellWidth + gridInfo.xOrigin),
          (int)(row * gridInfo.cellHeight + gridInfo.yOrigin),
          (int)((column + 1)* gridInfo.cellWidth + gridInfo.xOrigin),
          (int)((row + 1) * gridInfo.cellHeight + gridInfo.yOrigin)
          );

      LOG.info("Going to process this rectangle next round: "+rectProcessed);
      conf.set(SplitCalculator.QUERY_RANGE, rectProcessed.x1 + ","+rectProcessed.y1+","+
          rectProcessed.x2 +","+rectProcessed.y2);

      int round = 0;
      
      while (!jobFinished) {
        // Last argument is the base name output file
        Path outputPath = new Path(args[args.length - 1]+"_"+round);
        FileOutputFormat.setOutputPath(conf, outputPath);

        JobClient.runJob(conf);
        
        // Check that results are correct
        FileStatus[] resultFiles = fs.listStatus(outputPath);
        // Maximum distance of neighbors
        double farthestNeighbor = 0.0;
        for (FileStatus resultFile : resultFiles) {
          if (resultFile.getLen() > 0) {
            LineReader in = new LineReader(fs.open(resultFile.getPath()));
            Text line = new Text();
            while (in.readLine(line) > 0) {
              int i = 0;
              // Skip all characters till the -
              while (line.charAt(i++) != '-');
              // Parse the rest of the line to get the distance
              double distance = Double.parseDouble(new String(line.getBytes(), i, line.getLength() - i));
              if (distance > farthestNeighbor)
                farthestNeighbor = distance;
            }
            in.close();
          }
        }
        
        jobFinished = true;
        
        LOG.info("Farthest neighbor: "+farthestNeighbor);

        // Ensure that maximum distance cannot go outside current cell
        for (int i = 0; i < inputPaths.length; i++) {
          // Find cell that contains query point; the one that was actually processed
          if (gridInfo == null)
            continue;
          LOG.info("The cell that was processed: "+rectProcessed);
          double minDistance = rectProcessed.minDistance(queryPoint);
          LOG.info("Min distance within processed cell: "+minDistance);
          if (minDistance < farthestNeighbor) {
            // TODO ensure that there is another grid cell at that distance
            // This indicates that there might be a nearer neighbor in
            // an adjacent cell
            LOG.warn("Result is incorrect! farthestNeighbor: "+farthestNeighbor+", maxDistance: "+minDistance);
            
            // Add all grid cells that need to be processed
            for (double x = gridInfo.xOrigin; (x + gridInfo.cellWidth / 2) < gridInfo.xOrigin + gridInfo.gridWidth; x += gridInfo.cellWidth) {
              for (double y = gridInfo.yOrigin; (y + gridInfo.cellHeight / 2) < gridInfo.yOrigin + gridInfo.gridHeight; y += gridInfo.cellHeight) {
                Rectangle rect = new Rectangle(0, (int)x, (int)y, (int)x + (int)gridInfo.cellWidth, (int)x + (int)gridInfo.cellHeight);
                if (rect.minDistance(queryPoint) < farthestNeighbor && !rectProcessed.union(rect).equals(rectProcessed)) {
                  // Add this rectangle to the next processed items
                  rectProcessed = rectProcessed.union(rect);
                  jobFinished = false;
                }
              }
            }
          }
        }
        ++round;
        
      }
    }
}
