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
  
  public static CellInfo[] cellInfos;
  
  public static class Map extends MapReduceBase
  implements
  Mapper<LongWritable, TigerShape, CellInfo, TigerShape> {

    public void map(
        LongWritable id,
        TigerShape shape,
        OutputCollector<CellInfo, TigerShape> output,
        Reporter reporter) throws IOException {

      for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
        if (cellInfos[cellIndex].isIntersected(shape)) {
          output.collect(cellInfos[cellIndex], shape);
        }
      }
    }
  }

  public static void repartition(JobConf conf, Path inputFile, Path outputPath,
      GridInfo gridInfo, boolean pack, boolean rtree) throws IOException {
    conf.setJobName("Repartition");
    
    FileSystem inFileSystem = inputFile.getFileSystem(conf);
    FileSystem outFileSystem = outputPath.getFileSystem(conf);

    if (gridInfo == null)
      gridInfo = WriteGridFile.getGridInfo(inFileSystem, inputFile, outFileSystem);
    if (gridInfo.cellWidth == 0 || rtree)
      gridInfo.calculateCellDimensions(inFileSystem.getFileStatus(inputFile).getLen() * (rtree? 4 : 1), inFileSystem.getDefaultBlockSize());
    CellInfo[] cellsInfo = pack ?
        WriteGridFile.packInRectangles(inFileSystem, inputFile, outFileSystem, gridInfo) :
          gridInfo.getAllCells();

    // Overwrite output file
    if (inFileSystem.exists(outputPath)) {
      // remove the file first
      inFileSystem.delete(outputPath, true);
    }

    RepartitionInputFormat.setInputPaths(conf, inputFile);
    conf.setInputFormat(RepartitionInputFormat.class);
    conf.setOutputKeyClass(CellInfo.class);
    conf.setOutputValueClass(TigerShape.class);

    conf.setMapperClass(Map.class);

    // Set default parameters for reading input file
    conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShape.class.getName());
    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());

    FileOutputFormat.setOutputPath(conf,outputPath);
    conf.setOutputFormat(rtree ? RTreeGridOutputFormat.class : GridOutputFormat.class);
    conf.set(GridOutputFormat.OUTPUT_GRID, gridInfo.writeToString());
    conf.set(GridOutputFormat.OUTPUT_CELLS, GridOutputFormat.encodeCells(cellsInfo));

    JobClient.runJob(conf);
    
    // Rename output file to required name
    // Check that results are correct
    FileStatus[] resultFiles = inFileSystem.listStatus(outputPath);
    for (FileStatus resultFile : resultFiles) {
      if (resultFile.getLen() > 0) {
        // TODO whoever created this tempppp folder is responsible for deleting it
        Path temp = new Path("/tempppp");
        inFileSystem.rename(resultFile.getPath(), temp);
        
        inFileSystem.delete(outputPath, true);
        
        inFileSystem.rename(temp, outputPath);
      }
    }
  }
	
	/**
	 * Entry point to the file.
	 * Params grid:<gridInfo> [-pack] [-rtree] <input filenames> <output filename>
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
    
    boolean rtree = cla.isRtree();
    boolean pack = cla.isPack();
    
    repartition(conf, inputPath, outputPath, gridInfo, pack, rtree);
	}
}
