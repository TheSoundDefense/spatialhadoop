package edu.umn.cs.spatialHadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.GridInfo;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.PairShape;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeLineInputFormat;

/**
 * An implementation of Spatial Join MapReduce as it appears in
 * S. Zhang, J. Han, Z. Liu, K. Wang, and Z. Xu. SJMR:
 * Parallelizing spatial join with MapReduce on clusters. In
 * CLUSTER, pages 1–8, New Orleans, LA, Aug. 2009.
 * The map function partitions data into grid cells and the reduce function
 * makes a plane-sweep over each cell.
 * @author eldawy
 *
 */
public class SJMR {
  
  /**Class logger*/
  private static final Log LOG = LogFactory.getLog(SJMR.class);
  
  public static class IndexedText implements Writable {
    public byte index;
    public Text text;
    
    IndexedText() {
      text = new Text();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeByte(index);
      text.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      index = in.readByte();
      text.readFields(in);
    }
  }
  
  /**
   * The map class maps each object to all cells it overlaps with.
   * @author eldawy
   *
   */
  public static class SJMRMap extends MapReduceBase
  implements
  Mapper<LongWritable, Text, IntWritable, IndexedText> {
    /**List of cells used by the mapper*/
    private Shape shape;
    private IndexedText outputValue = new IndexedText();
    private CellInfo[] cellInfos;
    private IntWritable cellId = new IntWritable();
    private Path[] inputFiles;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      // Retrieve cells to use for partitioning
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      // Create a stock shape for deserializing lines
      shape = SpatialSite.createStockShape(job);
      // Get input paths to determine file index for every record
      inputFiles = FileInputFormat.getInputPaths(job);
    }

    @Override
    public void map(LongWritable dummy, Text value,
        OutputCollector<IntWritable, IndexedText> output,
        Reporter reporter) throws IOException {
      Text tempText = new Text(value);
      shape.fromText(tempText);
      FileSplit fsplit = (FileSplit) reporter.getInputSplit();
      for (int i = 0; i < inputFiles.length; i++) {
        if (inputFiles[i].equals(fsplit.getPath())) {
          outputValue.index = (byte) i;
        }
      }

      for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
        if (cellInfos[cellIndex].isIntersected(shape)) {
          cellId.set((int)cellInfos[cellIndex].cellId);
          outputValue.text = value;
          output.collect(cellId, outputValue);
        }
      }
    }
  }
  
  public static class SJMRReduce<S extends Shape> extends MapReduceBase implements
  Reducer<IntWritable, IndexedText, CellInfo, PairShape<S>> {
    /**Number of files in the input*/
    private int inputFileCount;
    
    /**List of cells used by the reducer*/
    private CellInfo[] cellInfos;

    private S shape;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      String cellsInfoStr = job.get(GridOutputFormat.OUTPUT_CELLS);
      cellInfos = GridOutputFormat.decodeCells(cellsInfoStr);
      shape = (S) SpatialSite.createStockShape(job);
      inputFileCount = FileInputFormat.getInputPaths(job).length;
    }

    @Override
    public void reduce(IntWritable cellId, Iterator<IndexedText> values,
        final OutputCollector<CellInfo, PairShape<S>> output, Reporter reporter)
        throws IOException {
      // Extract CellInfo (MBR) for duplicate avoidance checking
      int i_cell = 0;
      while (i_cell < cellInfos.length && cellInfos[i_cell].cellId != cellId.get())
        i_cell++;
      final CellInfo cellInfo = cellInfos[i_cell];
      
      // Partition retrieved shapes (values) into lists for each file
      @SuppressWarnings("unchecked")
      List<S>[] shapeLists = new List[inputFileCount];
      for (int i = 0; i < shapeLists.length; i++) {
        shapeLists[i] = new Vector<S>();
      }
      
      while (values.hasNext()) {
        IndexedText t = values.next();
        S s = (S) shape.clone();
        s.fromText(t.text);
        shapeLists[t.index].add(s);
      }
      
      final PairShape<S> value = new PairShape<S>();
      // Perform spatial join between the two lists
      SpatialAlgorithms.SpatialJoin_planeSweep(shapeLists[0], shapeLists[1], new SpatialAlgorithms.ResultCollector2<S, S>() {
        @Override
        public void add(S x, S y) {
          try {
            Rectangle intersectionMBR = x.getMBR().getIntersection(y.getMBR());
            if (cellInfo.contains(intersectionMBR.x, intersectionMBR.y)) {
              // Report to the reduce result collector
              value.first = x;
              value.second = y;
              output.collect(cellInfo, value);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  public static<S extends Shape> long sjmr(FileSystem fs, Path[] files,
      GridInfo gridInfo, S stockShape, OutputCollector<PairShape<CellInfo>,
      PairShape<? extends Shape>> output) throws IOException {
    JobConf job = new JobConf(SJMR.class);
    
    Path outputPath;
    FileSystem outFs = files[0].getFileSystem(job);
    do {
      outputPath = new Path("/" + files[0].getName() + ".sjmr_"
          + (int) (Math.random() * 1000000));
    } while (outFs.exists(outputPath));
    outFs.deleteOnExit(outputPath);
    
    ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
    job.setJobName("SJMR");
    job.setMapperClass(SJMRMap.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IndexedText.class);
    job.setNumMapTasks(5 * Math.max(1, clusterStatus.getMaxMapTasks()));
    job.setLong("mapred.min.split.size",
        Math.max(fs.getFileStatus(files[0]).getBlockSize(),
            fs.getFileStatus(files[1]).getBlockSize()));


    job.setReducerClass(SJMRReduce.class);
    job.setNumReduceTasks(Math.max(1, clusterStatus.getMaxReduceTasks()));

    job.setInputFormat(ShapeLineInputFormat.class);
    job.set(SpatialSite.SHAPE_CLASS, stockShape.getClass().getName());
    job.setOutputFormat(TextOutputFormat.class);
    
    String commaSeparatedFiles = "";
    for (int i = 0; i < files.length; i++) {
      if (i > 0)
        commaSeparatedFiles += ',';
      commaSeparatedFiles += files[i].toUri().toString();
    }
    ShapeLineInputFormat.addInputPaths(job, commaSeparatedFiles);
    
    // Calculate and set the dimensions of the grid to use in the map phase
    long total_size = 0;
    long max_size = 0;
    Path largest_file = null;
    for (Path file : files) {
      long size = fs.getFileStatus(file).getLen();
      total_size += size;
      if (size > max_size) {
        max_size = size;
        largest_file = file;
      }
    }
    // If the largest file is globally indexed, use its partitions
    BlockLocation[] fileBlockLocations =
      fs.getFileBlockLocations(fs.getFileStatus(largest_file), 0, max_size);
    CellInfo[] cellsInfo;
    if (false && fileBlockLocations[0].getCellInfo() != null) {
      Set<CellInfo> all_cells = new HashSet<CellInfo>();
      for (BlockLocation location : fileBlockLocations) {
        all_cells.add(location.getCellInfo());
      }
      cellsInfo = all_cells.toArray(new CellInfo[all_cells.size()]);
      job.setBoolean(SpatialSite.AutoCombineSplits, true);
    } else {
      total_size += total_size * job.getFloat(SpatialSite.INDEXING_OVERHEAD,
          0.002f);
      int num_cells = (int) (total_size / outFs.getDefaultBlockSize());
      gridInfo.calculateCellDimensions(num_cells);
      cellsInfo = gridInfo.getAllCells();
      job.setBoolean(SpatialSite.AutoCombineSplits, false);
    }
    job.set(GridOutputFormat.OUTPUT_CELLS,
        GridOutputFormat.encodeCells(cellsInfo));
    
    TextOutputFormat.setOutputPath(job, outputPath);
    
    // Start the job
    RunningJob runningJob = JobClient.runJob(job);
    Counters counters = runningJob.getCounters();
    Counter outputRecordCounter = counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
    final long resultCount = outputRecordCounter.getValue();

    // Read job result
    if (output != null) {
      FileStatus[] results = outFs.listStatus(outputPath);
      for (FileStatus fileStatus : results) {
        if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
          // Report every single result as a pair of shapes
          PairShape<CellInfo> cells =
              new PairShape<CellInfo>(new CellInfo(), new CellInfo());
          PairShape<? extends Shape> shapes =
              new PairShape<S>(stockShape, (S) stockShape.clone());
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          while (lineReader.readLine(text) > 0) {
            String str = text.toString();
            String[] parts = str.split("\t", 2);
            cells.fromText(new Text(parts[0]));
            shapes.fromText(new Text(parts[1]));
            output.collect(cells, shapes);
          }
          lineReader.close();
        }
      }
    }
    
    return resultCount;
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] inputPaths = cla.getPaths();
    JobConf conf = new JobConf(RedistributeJoin.class);
    FileSystem fs = inputPaths[0].getFileSystem(conf);
    GridInfo gridInfo = cla.getGridInfo();
    Shape stockShape = cla.getShape(true);
    if (gridInfo == null) {
      Rectangle rect = cla.getRectangle();
      if (rect == null) {
        rect = new Rectangle();
        for (Path path : inputPaths) {
          Rectangle file_mbr = FileMBR.fileMBRLocal(fs, path, stockShape);
          rect = rect.union(file_mbr);
        }
      }
      gridInfo = new GridInfo(rect.x, rect.y, rect.width, rect.height);
    }
    long t1 = System.currentTimeMillis();
    long resultSize = sjmr(fs, inputPaths, gridInfo, stockShape, null);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }

}
