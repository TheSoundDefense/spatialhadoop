package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.ClusterStatus;
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
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialSite;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.Estimator;
import edu.umn.cs.spatialHadoop.TigerShape;
import edu.umn.cs.spatialHadoop.mapReduce.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeInputFormat;

/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author aseldawy
 *
 */
public class Repartition {
  static final Log LOG = LogFactory.getLog(Repartition.class);
  
  static {
    Configuration.addDefaultResource("spatial-default.xml");
    Configuration.addDefaultResource("spatial-site.xml");
  }
  
  /**
   * The map class maps each object to all cells it overlaps with.
   * @author eldawy
   *
   */
  public static class RepartitionMap<T extends Shape> extends MapReduceBase
      implements Mapper<LongWritable, T, IntWritable, T> {
    /**List of cells used by the mapper*/
    private CellInfo[] cellInfos;
    
    /**Used to output intermediate records*/
    private IntWritable cellId = new IntWritable();
    
    @Override
    public void configure(JobConf job) {
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      super.configure(job);
    }
    
    /**
     * Map function
     * @param dummy
     * @param shape
     * @param output
     * @param reporter
     * @throws IOException
     */
    public void map(
        LongWritable dummy,
        T shape,
        OutputCollector<IntWritable, T> output,
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
    /**List of cells used by the reducer*/
    private CellInfo[] cellInfos;

    @Override
    public void configure(JobConf job) {
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      super.configure(job);
    }
    
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
   * Calculates number of partitions required to index the given file
   * @param inFs
   * @param file
   * @param rtree
   * @return
   * @throws IOException 
   */
  static int calculateNumberOfPartitions(FileSystem inFs, Path file,
      FileSystem outFs,
      boolean rtree) throws IOException {
    Configuration conf = inFs.getConf();
    final float ReplicationOverhead =
        conf.getFloat(SpatialSite.REPLICATION_OVERHEAD, 0.002f);
    final long fileSize = inFs.getFileStatus(file).getLen();
    if (!rtree) {
      long indexedFileSize = (long) (fileSize * (1 + ReplicationOverhead));
      return (int)Math.ceil((float)indexedFileSize / outFs.getDefaultBlockSize());
    } else {
      final int RTreeDegree = conf.getInt(SpatialSite.RTREE_DEGREE, 11);
      Class<? extends Shape> recordClass =
          conf.getClass(SpatialSite.SHAPE_CLASS, TigerShape.class).
          asSubclass(Shape.class);
      int record_size = 0;
      try {
        record_size = RTreeGridRecordWriter.calculateRecordSize(recordClass.newInstance());
      } catch (InstantiationException e1) {
        e1.printStackTrace();
      } catch (IllegalAccessException e1) {
        e1.printStackTrace();
      }
      long blockSize = conf.getLong(SpatialSite.RTREE_BLOCK_SIZE,
          outFs.getDefaultBlockSize());
      
      LOG.info("RTree block size: "+blockSize);
      final int records_per_block =
          RTree.getBlockCapacity(blockSize, RTreeDegree, record_size);
      LOG.info("RTrees can hold up to: "+records_per_block+" recods");
      
      final FSDataInputStream in = inFs.open(file);
      int blockCount;
      if (in.readLong() == SpatialSite.RTreeFileMarker) {
        blockCount = (int) Math.ceil((double)inFs.getFileStatus(file).getLen() /
            inFs.getFileStatus(file).getBlockSize());
      } else {
        Estimator<Integer> estimator = new Estimator<Integer>(0.01);
        estimator.setRandomSample(new Estimator.RandomSample() {
          
          @Override
          public double next() {
            int lineLength = 0;
            try {
              long randomFilePosition = (long)(Math.random() * fileSize);
              in.seek(randomFilePosition);
              
              // Skip the rest of this line
              byte lastReadByte;
              do {
                lastReadByte = in.readByte();
              } while (lastReadByte != '\n' && lastReadByte != '\r');
              
              while (in.getPos() < fileSize - 1) {
                lastReadByte = in.readByte();
                if (lastReadByte == '\n' || lastReadByte == '\r') {
                  break;
                }
                lineLength++;
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
            return lineLength+1;
          }
        });
        
        estimator.setUserFunction(new Estimator.UserFunction<Integer>() {
          @Override
          public Integer calculate(double x) {
            double lineCount = fileSize / x;
            double indexedRecordCount = lineCount * (1.0 + ReplicationOverhead);
            return (int) Math.ceil(indexedRecordCount / records_per_block);
          }
        });
        
        estimator.setQualityControl(new Estimator.QualityControl<Integer>() {
          @Override
          public boolean isAcceptable(Integer y1, Integer y2) {
            return (double)Math.abs(y2 - y1) / Math.min(y1, y2) < 0.01;
          }
        });
        
        Estimator.Range<Integer> blockCountRange = estimator.getEstimate();
        LOG.info("block count range ["+ blockCountRange.limit1 + ","
            + blockCountRange.limit2 + "]");
        blockCount = Math.max(blockCountRange.limit1, blockCountRange.limit2);
      }
      in.close();
      return blockCount;
    }
  }
	
	/**
   * Repartitions a file that is already in HDFS. It runs a MapReduce job
   * that partitions the file into cells, and writes each cell separately.
   * @param conf
   * @param inFile
   * @param outPath
   * @param gridInfo
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException
   */
  public static void repartitionMapReduce(Path inFile, Path outPath,
      GridInfo gridInfo, boolean pack, boolean rtree, boolean overwrite)
          throws IOException {
    
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileSystem outFs = outPath.getFileSystem(new Configuration());
    if (gridInfo == null) {
      Rectangle mbr = FileMBR.fileMBRLocal(inFs, inFile);
      gridInfo = new GridInfo(mbr.x, mbr.y, mbr.width, mbr.height);
    }
    if (gridInfo.columns == 0 || rtree) {
      // Recalculate grid dimensions
      int num_cells = calculateNumberOfPartitions(inFs, inFile, outFs, rtree);
      gridInfo.calculateCellDimensions(num_cells);
    }
    CellInfo[] cellInfos = pack ?
        packInRectangles(inFs, inFile, outFs, gridInfo) :
        gridInfo.getAllCells();
        
    repartitionMapReduce(inFile, outPath, cellInfos, pack, rtree, overwrite);
  }
  
  /**
   * Repartitions an input file according to the given list of cells.
   * @param inFile
   * @param outPath
   * @param cellInfos
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException
   */
  public static void repartitionMapReduce(Path inFile, Path outPath,
      CellInfo[] cellInfos, boolean pack, boolean rtree, boolean overwrite) throws IOException {
    JobConf job = new JobConf(Repartition.class);
    job.setJobName("Repartition");
    FileSystem outFs = outPath.getFileSystem(job);
    
    // Overwrite output file
    if (outFs.exists(outPath)) {
      if (overwrite)
        outFs.delete(outPath, true);
      else
        throw new RuntimeException("Output file '" + outPath
            + "' already exists and overwrite flag is not set");
    }
    
    // Decide which map function to use depending on how blocks are indexed
    // And also which input format to use
    job.setMapperClass(RepartitionMap.class);
    job.setInputFormat(ShapeInputFormat.class);
    ShapeInputFormat.setInputPaths(job, inFile);

    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(TigerShape.class);
    job.setBoolean(SpatialSite.AutoCombineSplits, true);
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
  
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
  
    // Set default parameters for reading input file
    job.set(SpatialSite.SHAPE_CLASS, TigerShape.class.getName());
  
    FileOutputFormat.setOutputPath(job,outPath);
    job.setOutputFormat(rtree ? RTreeGridOutputFormat.class : GridOutputFormat.class);
    job.set(GridOutputFormat.OUTPUT_CELLS,
        GridOutputFormat.encodeCells(cellInfos));
    job.setBoolean(GridOutputFormat.OVERWRITE, overwrite);
  
    JobClient.runJob(job);
    
    // Combine all output files into one file as we do with grid files
    Vector<Path> pathsToConcat = new Vector<Path>();
    FileStatus[] resultFiles = outFs.listStatus(outPath);
    for (int i = 0; i < resultFiles.length; i++) {
      FileStatus resultFile = resultFiles[i];
      if (resultFile.getLen() > 0 &&
          resultFile.getLen() % resultFile.getBlockSize() == 0) {
        Path partFile = new Path(outPath.toUri().getPath()+"_"+i);
        outFs.rename(resultFile.getPath(), partFile);
        LOG.info("Rename "+resultFile.getPath()+" -> "+partFile);
        pathsToConcat.add(partFile);
      }
    }
    
    LOG.info("Concatenating: "+pathsToConcat+" into "+outPath);
    if (outFs.exists(outPath))
      outFs.delete(outPath, true);
    if (pathsToConcat.size() == 1) {
      outFs.rename(pathsToConcat.firstElement(), outPath);
    } else if (!pathsToConcat.isEmpty()) {
      Path target = pathsToConcat.lastElement();
      pathsToConcat.remove(pathsToConcat.size()-1);
      outFs.concat(target,
          pathsToConcat.toArray(new Path[pathsToConcat.size()]));
      outFs.rename(target, outPath);
    }
  }
  
  public static CellInfo[] packInRectangles(FileSystem inFileSystem, Path inputPath,
      FileSystem outFileSystem, GridInfo gridInfo) throws IOException {
    return packInRectangles(inFileSystem, new Path[] {inputPath}, outFileSystem, gridInfo);
  }
  public static CellInfo[] packInRectangles(FileSystem fs, Path[] files,
      FileSystem outFileSystem, GridInfo gridInfo) throws IOException {
    final Vector<Point> sample = new Vector<Point>();
    long total_size = 0;
    for (Path file : files) {
      total_size += fs.getFileStatus(file).getLen();
    }
    long sample_size = total_size / 500;
    LOG.info("Reading a sample of size: "+sample_size + " bytes");
    Sampler.sampleLocalWithSize(fs, files, sample_size , new OutputCollector<LongWritable, TigerShape>(){
      @Override
      public void collect(LongWritable key, TigerShape value)
          throws IOException {
        sample.add(new Point(value.getX1(), value.getX2()));
      }
    }, new TigerShape());
    LOG.info("Finished reading a sample of size: "+sample.size()+" records");
    Rectangle[] rectangles = RTree.packInRectangles(gridInfo,
            sample.toArray(new Point[sample.size()]));
    CellInfo[] cellsInfo = new CellInfo[rectangles.length];
    for (int i = 0; i < rectangles.length; i++)
      cellsInfo[i] = new CellInfo(i, rectangles[i]);
    return cellsInfo;
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
    CommandLineArguments cla = new CommandLineArguments(args);
    Path inputPath = cla.getPaths()[0];
    Path outputPath = cla.getPaths()[1];
    
    GridInfo gridInfo = cla.getGridInfo();
    
    boolean rtree = cla.isRtree();
    boolean pack = cla.isPack();
    boolean overwrite = cla.isOverwrite();
    
    repartitionMapReduce(inputPath, outputPath, gridInfo, pack, rtree, overwrite);
	}
}
