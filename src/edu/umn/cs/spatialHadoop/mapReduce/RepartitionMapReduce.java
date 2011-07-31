package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.spatial.WriteGridFile;

import edu.umn.cs.CommandLineArguments;


/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author aseldawy
 *
 */
public class RepartitionMapReduce {
  public static final Log LOG = LogFactory.getLog(RepartitionMapReduce.class);
  
  public static GridInfo gridInfo;
  public static CellInfo[][] cellInfos;
  
  public static class Map extends MapReduceBase
  implements
  Mapper<LongWritable, TigerShape, CellInfo, TigerShape> {

    public void map(
        LongWritable id,
        TigerShape shape,
        OutputCollector<CellInfo, TigerShape> output,
        Reporter reporter) throws IOException {

      Rectangle rectangle = shape.getMBR();
      int cellCol1 = (int) ((rectangle.getX1() - gridInfo.xOrigin) / gridInfo.cellWidth);
      int cellRow1 = (int) ((rectangle.getY1() - gridInfo.yOrigin) / gridInfo.cellHeight);
      int cellCol2 = (int) ((rectangle.getX2() - gridInfo.xOrigin) / gridInfo.cellWidth);
      int cellRow2 = (int) ((rectangle.getY2() - gridInfo.yOrigin) / gridInfo.cellHeight);

      for (int cellCol = cellCol1; cellCol <= cellCol2; cellCol++) {
        for (int cellRow = cellRow1; cellRow <= cellRow2; cellRow++) {
          CellInfo cellInfo = cellInfos[cellCol][cellRow];
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
    if (gridInfo == null)
      gridInfo = WriteGridFile.getGridInfo(fileSystem, inputFile, fileSystem);
    if (gridInfo.cellWidth == 0)
      gridInfo.calculateCellDimensions(fileSystem.getFileStatus(inputFile).getLen(), fileSystem.getDefaultBlockSize());

    // Save gridInfo in job configuration
    conf.set(GridOutputFormat.OUTPUT_GRID, gridInfo.writeToString());
    
    // Overwrite output file
    if (fileSystem.exists(outputPath)) {
      // remove the file first
      fileSystem.delete(outputPath, true);
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
    CommandLineArguments cla = new CommandLineArguments(args);
    Path inputPath = cla.getInputPath();
    Path outputPath = cla.getOutputPath();
    
    GridInfo gridInfo = cla.getGridInfo();
    
    repartition(conf, inputPath, outputPath, gridInfo);
	}
}
