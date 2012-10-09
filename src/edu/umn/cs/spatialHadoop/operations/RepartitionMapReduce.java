package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.spatial.WriteGridFile;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.GridOutputFormat;

/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author aseldawy
 *
 */
public class RepartitionMapReduce {
  static final Log LOG = LogFactory.getLog(RepartitionMapReduce.class);
  
  /**Configuration line name for replication overhead*/
  public static final String REPLICATION_OVERHEAD =
      "spatialHadoop.storage.ReplicationOverHead";
  
  /**List of cells used by the mapper and the reducer*/
  static CellInfo[] cellInfos;
  
  /**
   * A custom input format that sets cellInfos before starting the mapper
   * @author eldawy
   *
   */
  public static class InputFormat extends ShapeInputFormat {
    @Override
    public RecordReader<LongWritable, Shape> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      return super.getRecordReader(split, job, reporter);
    }
  }
  
  /**
   * An output committer that sets cellInfos before starting the reducer
   * @author eldawy
   *
   */
  public static class OutputCommitter extends FileOutputCommitter {
    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
      String cellsInfoStr = 
          context.getConfiguration().get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      super.setupTask(context);
    }
  }
  
  /**
   * The map class maps each object to all cells it overlaps with.
   * @author eldawy
   *
   */
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
  
  /**
   * The reducer writes records to the cell they belong to. It also  finializes
   * the cell by writing a <code>null</code> object after all objects.
   * @author eldawy
   *
   */
  public static class Reduce extends MapReduceBase implements
  Reducer<IntWritable, TigerShape, CellInfo, TigerShape> {
    @Override
    public void reduce(IntWritable cellId, Iterator<TigerShape> values,
        OutputCollector<CellInfo, TigerShape> output, Reporter reporter)
            throws IOException {
      CellInfo cellInfo = null;
      for (CellInfo _cellInfo : cellInfos) {
        if (_cellInfo.cellId == cellId.get())
          cellInfo = _cellInfo;
      }
      // If writing to a grid file, concatenated in text
      while (values.hasNext()) {
        TigerShape value = values.next();
        output.collect(cellInfo, value);
      }
      // Close this cell as we will not write any more data to it
      output.collect(cellInfo, null);
    }
  }
  
  /**
   * Repartitions a file that is already in HDFS. It runs a MapReduce job
   * that partitions the file into cells, and writes each cell separately.
   * @param conf
   * @param inputFile
   * @param outputPath
   * @param gridInfo
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException
   */
  public static void repartition(JobConf conf, Path inputFile, Path outputPath,
      GridInfo gridInfo, boolean pack, boolean rtree, boolean overwrite)
          throws IOException {
    conf.setJobName("Repartition");
    
    FileSystem inFileSystem = inputFile.getFileSystem(conf);
    FileSystem outFileSystem = outputPath.getFileSystem(conf);
    long outputBlockSize = outFileSystem.getDefaultBlockSize();

    if (gridInfo == null)
      gridInfo = WriteGridFile.getGridInfo(inFileSystem, inputFile, outFileSystem);
    if (gridInfo.columns == 0 || rtree) {
      final float ReplicationOverhead =
          conf.getFloat(REPLICATION_OVERHEAD, 0.001f);
      final int RTreeDegree = conf.getInt(RTreeGridRecordWriter.RTREE_DEGREE, 5);
      // Recalculate grid dimensions
      int num_cells;
      if (!rtree) {
        long source_file_size = inFileSystem.getFileStatus(inputFile).getLen();
        long dest_file_size = (long) (source_file_size * (1.0 + ReplicationOverhead));
        num_cells = (int)Math.ceil((float)dest_file_size / outputBlockSize);
      } else {
        long source_record_count = 
            LineCount.lineCountLocal(inFileSystem, inputFile);
        long dest_recourd_count = 
            (long)(source_record_count* (1.0 + ReplicationOverhead));
        Class<? extends Shape> recordClass =
            conf.getClass(ShapeRecordReader.SHAPE_CLASS, TigerShape.class).
            asSubclass(Shape.class);
        int record_size = RTreeGridRecordWriter.calculateRecordSize(recordClass);
        int records_per_block =
            RTree.getBlockCapacity(outputBlockSize, RTreeDegree, record_size);
        num_cells = (int) Math.ceil((float)dest_recourd_count / records_per_block);
      }
      gridInfo.calculateCellDimensions(num_cells);
    }
    CellInfo[] cellsInfo = pack ?
        WriteGridFile.packInRectangles(inFileSystem, inputFile, outFileSystem, gridInfo) :
          gridInfo.getAllCells();

    // Overwrite output file
    if (inFileSystem.exists(outputPath) && !overwrite) {
      throw new RuntimeException("Output file '" + outputPath
          + "' already exists and overwrite flag is not set");
    }

    conf.setInputFormat(InputFormat.class);
    InputFormat.setInputPaths(conf, inputFile);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(TigerShape.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    // Set default parameters for reading input file
    conf.set(ShapeRecordReader.SHAPE_CLASS, TigerShape.class.getName());

    FileOutputFormat.setOutputPath(conf,outputPath);
    conf.setOutputFormat(rtree ? RTreeGridOutputFormat.class : GridOutputFormat.class);
    conf.setOutputCommitter(OutputCommitter.class);
    conf.set(GridOutputFormat.OUTPUT_CELLS,
        GridOutputFormat.encodeCells(cellsInfo));
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
	 * ... grid:<gridInfo> [-pack] [-rtree] <input filenames> <output filename>
	 * gridInfo in the format <x1,y1,w,h[,cw,ch]>
	 * input filenames: Input file in HDFS
	 * output filename: Output file in HDFS
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
