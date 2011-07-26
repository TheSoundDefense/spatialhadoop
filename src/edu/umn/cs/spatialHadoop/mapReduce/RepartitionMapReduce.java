package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Point;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.spatialHadoop.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.TigerShapeWithIndex;


/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author aseldawy
 *
 */
public class RepartitionMapReduce {
  
  public static GridInfo gridInfo;
  public static CellInfo[][] cellInfos;
  
  public static class Map extends MapReduceBase
  implements
  Mapper<LongWritable, TigerShape, CellInfo, TigerShape> {

    private CellInfo getCellInfo(long x, long y) {
      return cellInfos[(int) ((x - gridInfo.xOrigin) / gridInfo.cellWidth)]
                       [(int) ((y - gridInfo.yOrigin) / gridInfo.cellHeight)];
    }

    public void map(
        LongWritable id,
        TigerShape shape,
        OutputCollector<CellInfo, TigerShape> output,
        Reporter reporter) throws IOException {

      for (long x = shape.getMBR().getX1(); x < shape.getMBR().getX2(); x += gridInfo.cellWidth) {
        for (long y = shape.getMBR().getY1(); y < shape.getMBR().getY2(); y += gridInfo.cellHeight) {
          CellInfo cellInfo = getCellInfo(x, y);
          output.collect(cellInfo, shape);
        }
      }
    }
  }

	public static void repartition(JobConf conf, Path inputFile, Path outputPath, GridInfo gridInfo) throws IOException {
    conf.setJobName("Repartition");
    
    FileSystem fileSystem = FileSystem.get(conf);
    // Automatically calculate recommended cell dimensions if not set
    // Calculate appropriate values for cellWidth, cellHeight based on file size
    // only if they're missing.
    // Note that we use default block size because we really care more about
    // output file block size rather than input file block size.
    if (gridInfo.cellWidth == 0)
      gridInfo.calculateCellDimensions(fileSystem.getFileStatus(inputFile).getLen(), fileSystem.getDefaultBlockSize());

    // Save gridInfo in job configuration
    conf.set(GridOutputFormat.OUTPUT_GRID, gridInfo.writeToString());
    
    // Overwrite output file
    if (fileSystem.exists(outputPath)) {
      // remove the file first
      fileSystem.delete(outputPath, false);
    }

    // add this query file as the first input path to the job
    RepartitionInputFormat.setInputPaths(conf, inputFile);
    
    conf.setOutputKeyClass(CellInfo.class);
    conf.setOutputValueClass(TigerShape.class);

    conf.setMapperClass(Map.class);

    // Set default parameters for reading input file
    conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShape.class.getName());
    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());

    conf.setInputFormat(RepartitionInputFormat.class);
    conf.setOutputFormat(GridOutputFormat.class);

    // Last argument is the output file
    FileOutputFormat.setOutputPath(conf,outputPath);

    JobClient.runJob(conf);
    
    // Rename output file to required name
    // Check that results are correct
    FileStatus[] resultFiles = fileSystem.listStatus(outputPath);
    for (FileStatus resultFile : resultFiles) {
      if (resultFile.getLen() > 0) {
        Path temp = new Path("/tempppp");
        fileSystem.rename(resultFile.getPath(), temp);
        
        fileSystem.delete(outputPath, true);
        
        fileSystem.rename(temp, outputPath);
      }
    }
  }
	
	/**
	 * Entry point to the file.
	 * Params <gridInfo> <input filenames> <output filename>
	 * gridInfo in the format <x1,y1,w,h,cw,ch>
	 * input filenames: Input file in HDFS
	 * output filename: Outputfile in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(RepartitionMapReduce.class);
    Path inputFile = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    // Retrieve gridInfo from parameters
    GridInfo gridInfo = new GridInfo();
    gridInfo.readFromString(args[0]);
    
    repartition(conf, inputFile, outputPath, gridInfo);
	}
}
