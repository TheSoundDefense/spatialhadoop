package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.spatial.RTree;
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
  
  private static CellInfo[] cellInfos;
  
  public static void setCellInfos(CellInfo[] cellInfos) {
    LOG.info("Mapping according to "+cellInfos.length+" cells");
    for (CellInfo cellInfo : cellInfos) {
      LOG.info("Mapping according to cell "+cellInfo);
    }
    RepartitionMapReduce.cellInfos = cellInfos;
  }
  
  public static class Map extends MapReduceBase
  implements
  Mapper<LongWritable, TigerShape, IntWritable, TigerShape> {

    static IntWritable cellId = new IntWritable();
    public void map(
        LongWritable id,
        TigerShape shape,
        OutputCollector<IntWritable, TigerShape> output,
        Reporter reporter) throws IOException {

      for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
        if (cellInfos[cellIndex].isIntersected(shape)) {
          cellId.set((int)cellInfos[cellIndex].cellId);
          output.collect(cellId, shape);
        }
      }
    }
  }
  
  public static class Reduce extends MapReduceBase implements
  Reducer<IntWritable, TigerShape, CellInfo, Text> {
    @Override
    public void reduce(IntWritable cellId, Iterator<TigerShape> values,
        OutputCollector<CellInfo, Text> output, Reporter reporter)
            throws IOException {
      CellInfo cellInfo = null;
      for (CellInfo _cellInfo : cellInfos) {
        if (_cellInfo.cellId == cellId.get())
          cellInfo = _cellInfo;
      }
      // If writing to a grid file, concatenated in text
      Text text = new Text();
      while (values.hasNext()) {
        text.clear();
        values.next().toText(text);
        output.collect(cellInfo, text);
      }
      // Close this cell as we will not write any more data to it
      output.collect(cellInfo, null);
    }
  }
  
  public static class RTreeReduce extends MapReduceBase implements
  Reducer<CellInfo, TigerShape, CellInfo, BytesWritable> {
    /**Maximum number of entries per RTree*/
    private static final int RTREE_LIMIT = 10000;
    private static final int RTREE_DEGREE = 5;

    @Override
    public void reduce(CellInfo cellInfo, Iterator<TigerShape> values,
        OutputCollector<CellInfo, BytesWritable> output, Reporter reporter)
            throws IOException {

      // Hold values and insert every chunk of RTREE_LIMIT shapes to
      // a single RTree and write to disk
      List<TigerShape> shapes = new Vector<TigerShape>();
      while (values.hasNext()) {
        TigerShape nextShape = (TigerShape) values.next().clone();
        shapes.add(nextShape);
        if (shapes.size() >= RTREE_LIMIT || !values.hasNext()) {
          RTree<TigerShape> concatenatedRTree = new RTree<TigerShape>();
          concatenatedRTree.bulkLoad(
              shapes.toArray(new TigerShape[shapes.size()]), RTREE_DEGREE);
          // Write data in current RTree
          ByteArrayOutputStream os = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(os);
          concatenatedRTree.write(dos);
          dos.close();
          BytesWritable buffer = new BytesWritable(os.toByteArray());
          output.collect(cellInfo, buffer);
          shapes.clear();
        }
      }
    }
  }

  public static void repartition(JobConf conf, Path inputFile, Path outputPath,
      GridInfo gridInfo, boolean pack, boolean rtree, boolean overwrite) throws IOException {
    conf.setJobName("Repartition");
    
    FileSystem inFileSystem = inputFile.getFileSystem(conf);
    FileSystem outFileSystem = outputPath.getFileSystem(conf);
    long outputBlockSize = outFileSystem.getDefaultBlockSize();

    if (gridInfo == null)
      gridInfo = WriteGridFile.getGridInfo(inFileSystem, inputFile, outFileSystem);
    if (gridInfo.columns == 0 || rtree)
      gridInfo.calculateCellDimensions(inFileSystem.getFileStatus(inputFile).getLen() * (rtree? 4 : 1), outputBlockSize);
    CellInfo[] cellsInfo = pack ?
        WriteGridFile.packInRectangles(inFileSystem, inputFile, outFileSystem, gridInfo) :
          gridInfo.getAllCells();

    // Overwrite output file
    if (inFileSystem.exists(outputPath) && !overwrite) {
      throw new RuntimeException("Output file '" + outputPath
          + "' already exists and overwrite flag is not set");
    }

    RepartitionInputFormat.setInputPaths(conf, inputFile);
    conf.setInputFormat(RepartitionInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(TigerShape.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(rtree ? RTreeReduce.class : Reduce.class);

    // Set default parameters for reading input file
    conf.set(TigerShapeRecordReader.TIGER_SHAPE_CLASS, TigerShape.class.getName());
    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());

    FileOutputFormat.setOutputPath(conf,outputPath);
    conf.setOutputFormat(rtree ? RTreeGridOutputFormat.class : GridOutputFormat.class);
    conf.setOutputFormat(rtree ? RPRTreeGridOutputFormat.class : RPGridOutputFormat.class);
    conf.set(GridOutputFormat.OUTPUT_GRID, gridInfo.writeToString());
    conf.set(GridOutputFormat.OUTPUT_CELLS, GridOutputFormat.encodeCells(cellsInfo));
    conf.setBoolean(GridOutputFormat.OVERWRITE, overwrite);

    JobClient.runJob(conf);
    
    // Combine all output files into one file as we do with grid files
    Vector<Path> pathsToConcat = new Vector<Path>();
    FileStatus[] resultFiles = inFileSystem.listStatus(outputPath);
    for (int i = 0; i < resultFiles.length; i++) {
      FileStatus resultFile = resultFiles[i];
      if (resultFile.getLen() > 0 &&
          resultFile.getLen() % resultFile.getBlockSize() == 0) {
        Path partFile = new Path(outputPath.toUri().getPath()+"_"+i);
        outFileSystem.rename(resultFile.getPath(), partFile);
        LOG.info("Rename "+resultFile.getPath()+" -> "+partFile);
        pathsToConcat.add(partFile);
      }
    }
    
    LOG.info("Concatenating: "+pathsToConcat+" into "+outputPath);
    if (outFileSystem.exists(outputPath))
      outFileSystem.delete(outputPath, true);
    if (pathsToConcat.size() == 1) {
      outFileSystem.rename(pathsToConcat.firstElement(), outputPath);
    } else if (!pathsToConcat.isEmpty()) {
      Path target = pathsToConcat.lastElement();
      pathsToConcat.remove(pathsToConcat.size()-1);
      outFileSystem.concat(target,
          pathsToConcat.toArray(new Path[pathsToConcat.size()]));
      outFileSystem.rename(target, outputPath);
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
    boolean overwrite = cla.isOverwrite();
    
    repartition(conf, inputPath, outputPath, gridInfo, pack, rtree, overwrite);
	}
}
