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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text2;
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
import org.apache.hadoop.spatial.ShapeRecordWriter;
import org.apache.hadoop.spatial.SpatialSite;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.GridRecordWriter;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeGridRecordWriter;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeRecordReader;

/**
 * Repartitions a file according to a different grid through a MapReduce job
 * @author aseldawy
 *
 */
public class Repartition {
  static final Log LOG = LogFactory.getLog(Repartition.class);
  
  /**
   * The map class maps each object to all cells it overlaps with.
   * @author eldawy
   *
   */
  public static class RepartitionMap<T extends Shape> extends MapReduceBase
      implements Mapper<LongWritable, T, IntWritable, Text> {
    /**List of cells used by the mapper*/
    private CellInfo[] cellInfos;
    
    /**Used to output intermediate records*/
    private IntWritable cellId = new IntWritable();
    
    /**Text representation of a shape*/
    private Text shapeText = new Text();
    
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
        OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {

      shapeText.clear();
      shape.toText(shapeText);
      for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
        if (cellInfos[cellIndex].isIntersected(shape)) {
          cellId.set((int)cellInfos[cellIndex].cellId);
          output.collect(cellId, shapeText);
        }
      }
    }
  }
  
  public static class Combine extends MapReduceBase implements
  Reducer<IntWritable, Text, IntWritable, Text> {
    private static final byte[] NEW_LINE = {'\n'};
    
    @Override
    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      Text combinedText = new Text2();
      while (values.hasNext()) {
        Text t = values.next();
        combinedText.append(t.getBytes(), 0, t.getLength());
        combinedText.append(NEW_LINE, 0, NEW_LINE.length);
      }
      combinedText = new Text(combinedText);
      output.collect(key, combinedText);
    }
  }
  
  /**
   * The reducer writes records to the cell they belong to. It also  finializes
   * the cell by writing a <code>null</code> object after all objects.
   * @author eldawy
   *
   */
  public static class Reduce extends MapReduceBase implements
  Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable cellId, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
      while (values.hasNext()) {
        Text value = values.next();
        output.collect(cellId, value);
      }
      // Close this cell as we will not write any more data to it
      output.collect(cellId, new Text());
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
      FileSystem outFs, long blockSize) throws IOException {
    Configuration conf = inFs.getConf();
    final float IndexingOverhead =
        conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f);
    final long fileSize = inFs.getFileStatus(file).getLen();
    long indexedFileSize = (long) (fileSize * (1 + IndexingOverhead));
    if (blockSize == 0)
      blockSize = outFs.getDefaultBlockSize();
    return (int)Math.ceil((float)indexedFileSize / blockSize);
  }
	
	/**
   * Repartitions a file that is already in HDFS. It runs a MapReduce job
   * that partitions the file into cells, and writes each cell separately.
   * @param conf
   * @param inFile
   * @param outPath
   * @param gridInfo
	 * @param stockShape 
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException
   */
  public static void repartitionMapReduce(Path inFile, Path outPath,
      GridInfo gridInfo, Shape stockShape, long blockSize,
      boolean pack, boolean rtree, boolean overwrite)
          throws IOException {
    
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileSystem outFs = outPath.getFileSystem(new Configuration());
    if (gridInfo == null) {
      Rectangle mbr = FileMBR.fileMBRMapReduce(inFs, inFile);
      gridInfo = new GridInfo(mbr.x, mbr.y, mbr.width, mbr.height);
    }
    
    if (gridInfo.columns == 0 || rtree) {
      // Recalculate grid dimensions
      int num_cells = calculateNumberOfPartitions(
          inFs, inFile, outFs, blockSize);
      gridInfo.calculateCellDimensions(num_cells);
    }
    LOG.info("Calculated grid: "+gridInfo);
    CellInfo[] cellInfos = pack ?
        packInRectangles(inFs, inFile, outFs, gridInfo, stockShape, false) :
        gridInfo.getAllCells();
        
    repartitionMapReduce(inFile, outPath, cellInfos, stockShape, blockSize,
        pack, rtree, overwrite);
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
      CellInfo[] cellInfos, Shape stockShape, long blockSize,
      boolean pack, boolean rtree, boolean overwrite) throws IOException {
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
    job.setMapOutputValueClass(Text.class);
    job.setBoolean(SpatialSite.AutoCombineSplits, true);
    job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
  
//    job.setCombinerClass(Combine.class);
    
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));
  
    // Set default parameters for reading input file
    job.set(SpatialSite.SHAPE_CLASS, stockShape.getClass().getName());
  
    FileOutputFormat.setOutputPath(job,outPath);
    job.setOutputFormat(rtree ? RTreeGridOutputFormat.class : GridOutputFormat.class);
    if (blockSize != 0)
      job.setLong(SpatialSite.LOCAL_INDEX_BLOCK_SIZE, blockSize);
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
  
  public static <S extends Shape> CellInfo[] packInRectangles(
      FileSystem inFileSystem, Path inputPath, FileSystem outFileSystem,
      GridInfo gridInfo, S stockShape, boolean local) throws IOException {
    return packInRectangles(inFileSystem, new Path[] { inputPath },
        outFileSystem, gridInfo, stockShape, local);
  }
  
  public static <S extends Shape> CellInfo[] packInRectangles(FileSystem fs,
      Path[] files, FileSystem outFileSystem, GridInfo gridInfo, S stockShape,
      boolean local)
      throws IOException {
    final Vector<Point> sample = new Vector<Point>();
    
    double sample_ratio =
        outFileSystem.getConf().getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    
    LOG.info("Reading a sample of "+(int)(sample_ratio*100) + "%");
    if (local) {
      Sampler.sampleLocalWithRatio(fs, files, sample_ratio , new OutputCollector<LongWritable, S>(){
        @Override
        public void collect(LongWritable key, S value)
            throws IOException {
          sample.add(new Point(value.getMBR().getX1(), value.getMBR().getY1()));
        }
      }, stockShape);
    } else {
      Sampler.sampleMapReduceWithRatio(fs, files, sample_ratio , new OutputCollector<LongWritable, S>(){
        @Override
        public void collect(LongWritable key, S value)
            throws IOException {
          sample.add(new Point(value.getMBR().getX1(), value.getMBR().getY1()));
        }
      }, stockShape);
    }
    LOG.info("Finished reading a sample of size: "+sample.size()+" records");
    Rectangle[] rectangles = RTree.packInRectangles(gridInfo,
            sample.toArray(new Point[sample.size()]));
    CellInfo[] cellsInfo = new CellInfo[rectangles.length];
    for (int i = 0; i < rectangles.length; i++)
      cellsInfo[i] = new CellInfo(i, rectangles[i]);
    
    return cellsInfo;
  }
  
  public static<S extends Shape> void repartitionLocal(Path inFile, Path outPath,
      GridInfo gridInfo, S stockShape, long blockSize,
      boolean pack, boolean rtree, boolean overwrite)
          throws IOException {
    
    FileSystem inFs = inFile.getFileSystem(new Configuration());
    FileSystem outFs = outPath.getFileSystem(new Configuration());
    if (gridInfo == null) {
      Rectangle mbr = FileMBR.fileMBRLocal(inFs, inFile);
      gridInfo = new GridInfo(mbr.x, mbr.y, mbr.width, mbr.height);
    }
    if (gridInfo.columns == 0 || rtree) {
      // Recalculate grid dimensions
      int num_cells = calculateNumberOfPartitions(
          inFs, inFile, outFs, blockSize);
      gridInfo.calculateCellDimensions(num_cells);
    }
    CellInfo[] cellInfos = pack ?
        packInRectangles(inFs, inFile, outFs, gridInfo, stockShape, true) :
        gridInfo.getAllCells();
        
    repartitionLocal(inFile, outPath, cellInfos, stockShape, blockSize,
        pack, rtree, overwrite);
  }

  /**
   * Repartitions a file on local machine without MapReduce jobs.
   * @param inFs
   * @param in
   * @param outFs
   * @param out
   * @param cells
   * @param stockShape
   * @param pack
   * @param rtree
   * @param overwrite
   * @throws IOException 
   */
  public static<S extends Shape> void repartitionLocal(Path in, Path out,
      CellInfo[] cells, S stockShape, long blockSize,
      boolean pack, boolean rtree, boolean overwrite) throws IOException {
    FileSystem inFs = in.getFileSystem(new Configuration());
    FileSystem outFs = out.getFileSystem(new Configuration());
    // Overwrite output file
    if (outFs.exists(out)) {
      if (overwrite)
        outFs.delete(out, true);
      else
        throw new RuntimeException("Output file '" + out
            + "' already exists and overwrite flag is not set");
    }
    
    ShapeRecordWriter<Shape> writer;
    if (rtree) {
      writer = new RTreeGridRecordWriter(outFs, out, cells, overwrite);
    } else {
      writer = new GridRecordWriter(outFs, out, cells, overwrite);
    }
    
    if (blockSize != 0)
      ((GridRecordWriter)writer).setBlockSize(blockSize);
    
    long length = inFs.getFileStatus(in).getLen();
    FSDataInputStream datain = inFs.open(in);
    ShapeRecordReader<S> reader = new ShapeRecordReader<S>(datain, 0, length);
    LongWritable k = reader.createKey();
    
    while (reader.next(k, stockShape)) {
      writer.write(k, stockShape);
    }
    writer.close(null);
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
    boolean local = cla.isLocal();
    long blockSize = cla.getBlockSize();
    Shape stockShape = cla.getShape(true);
    CellInfo[] cells = cla.getCells();
    
    if (cells != null) {
      if (blockSize == 0) {
        // Calculate block size based on overlap between given cells and
        // file mbr
        FileSystem fs = inputPath.getFileSystem(new Configuration());
        Rectangle mbr = local ? FileMBR.fileMBRLocal(fs, inputPath):
          FileMBR.fileMBRMapReduce(fs, inputPath);
        double overlap_area = 0;
        for (CellInfo cell : cells) {
          Rectangle overlap = mbr.getIntersection(cell);
          overlap_area += overlap.width * overlap.height;
        }
        long estimatedRepartitionedFileSize = (long) (fs.getFileStatus(
            inputPath).getLen() * overlap_area / (mbr.width * mbr.height));
        blockSize = estimatedRepartitionedFileSize / cells.length;
        // Adjust blockSize to a multiple of bytes per checksum
        int bytes_per_checksum =
            new Configuration().getInt("io.bytes.per.checksum", 512);
        blockSize = (long) (Math.ceil((double)blockSize / bytes_per_checksum) *
            bytes_per_checksum);
        LOG.info("Calculated block size: "+blockSize);
      }
      if (local)
        repartitionLocal(inputPath, outputPath, cells, stockShape,
            blockSize, pack, rtree, overwrite);
      else
        repartitionMapReduce(inputPath, outputPath, cells, stockShape,
            blockSize, pack, rtree, overwrite);
    } else {
      if (local)
        repartitionLocal(inputPath, outputPath, gridInfo, stockShape,
            blockSize, pack, rtree, overwrite);
      else
        repartitionMapReduce(inputPath, outputPath, gridInfo, stockShape,
            blockSize, pack, rtree, overwrite);
    }
	}
}
