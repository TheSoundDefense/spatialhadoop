package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.SpatialAlgorithms;
import org.apache.hadoop.spatial.SpatialSite;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.TigerShape;
import edu.umn.cs.spatialHadoop.mapReduce.Pair;
import edu.umn.cs.spatialHadoop.mapReduce.PairInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.PairOfFileSplits;
import edu.umn.cs.spatialHadoop.mapReduce.PairRecordReader;
import edu.umn.cs.spatialHadoop.mapReduce.PairShape;
import edu.umn.cs.spatialHadoop.mapReduce.PairWritable;
import edu.umn.cs.spatialHadoop.mapReduce.PairWritableComparable;
import edu.umn.cs.spatialHadoop.mapReduce.RTreeRecordReader;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeArrayRecordReader;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 * @author eldawy
 *
 */
public class RedistributeJoin {
  private static final Log LOG = LogFactory.getLog(RedistributeJoin.class);

  public static class RedistributeJoinMap extends MapReduceBase
  implements Mapper<PairWritableComparable<CellInfo>, PairWritable<? extends Writable>, PairWritableComparable<CellInfo>, PairWritable<? extends Shape>> 
  {
    public void map(
        final PairWritableComparable<CellInfo> key,
        final PairWritable<? extends Writable> value,
        final OutputCollector<PairWritableComparable<CellInfo>, PairWritable<? extends Shape>> output,
        Reporter reporter) throws IOException {
      final PairWritable<Shape> output_value = new PairWritable<Shape>();
      int result_size;
      
      if (value.first instanceof RTree) {
        @SuppressWarnings("unchecked")
        RTree<Shape> r1 = (RTree<Shape>) value.first;
        @SuppressWarnings("unchecked")
        RTree<Shape> r2 = (RTree<Shape>) value.second;
        result_size = RTree.spatialJoin(r1, r2,
            new RTree.ResultCollector2<Shape, Shape>() {
          @Override
          public void add(Shape x, Shape y) {
            output_value.first = x;
            output_value.second = y;
            try {
              output.collect(key, output_value);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      //} else if (value.first instanceof ArrayWritable){
        // Join two arrays
      } else {
        ArrayWritable ar1 = (ArrayWritable) value.first;
        ArrayWritable ar2 = (ArrayWritable) value.second;
        result_size = SpatialAlgorithms.SpatialJoin_planeSweep(
            (Shape[])ar1.get(), (Shape[])ar2.get(),
            new SpatialAlgorithms.ResultCollector2<Shape, Shape>() {
          @Override
          public void add(Shape x, Shape y) {
            output_value.first = x;
            output_value.second = y;
            try {
              output.collect(key, output_value);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      }
      LOG.info("Found: "+result_size+" pairs");
    }
  }
  
  /**
   * Record reader that reads pair of cell infos and pair of RTrees
   * @author eldawy
   *
   */
  public static class DJRecordReaderTree<S extends Shape> extends PairRecordReader<CellInfo, RTree<S>> {
    public DJRecordReaderTree(Configuration conf, PairOfFileSplits fileSplits) throws IOException {
      this.conf = conf;
      this.splits = fileSplits;
      this.internalReaders = new Pair<RecordReader<CellInfo,RTree<S>>>();
      this.internalReaders.first = createRecordReader(this.conf, this.splits.first);
      this.internalReaders.second = createRecordReader(this.conf, this.splits.second);
    }
    
    @Override
    protected RecordReader<CellInfo, RTree<S>> createRecordReader(
        Configuration conf, FileSplit split) throws IOException {
      return new RTreeRecordReader<S>(conf, split);
    }
  }

  /**
   * An input format that creates record readers that read a pair of cells
   * and a pair of RTrees.
   * @author eldawy
   *
   */
  public static class DJInputFormatTree<S extends Shape> extends PairInputFormat<CellInfo, RTree<S>> {
    @Override
    public RecordReader<PairWritableComparable<CellInfo>, PairWritable<RTree<S>>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new DJRecordReaderTree<S>(job, (PairOfFileSplits)split);
    }
  }

  /**
   * Reads a pair of arrays of shapes
   * @author eldawy
   *
   */
  public static class DJRecordReaderArray extends PairRecordReader<CellInfo, ArrayWritable> {
    public DJRecordReaderArray(Configuration conf, PairOfFileSplits fileSplits) throws IOException {
      this.conf = conf;
      this.splits = fileSplits;
      this.internalReaders = new Pair<RecordReader<CellInfo,ArrayWritable>>();
      this.internalReaders.first = createRecordReader(this.conf, this.splits.first);
      this.internalReaders.second = createRecordReader(this.conf, this.splits.second);
    }
    
    @Override
    protected RecordReader<CellInfo, ArrayWritable> createRecordReader(
        Configuration conf, FileSplit split) throws IOException {
      return new ShapeArrayRecordReader(conf, split);
    }
  }

  /**
   * Input format that returns a record reader that reads a pair of arrays of
   * shapes
   * @author eldawy
   *
   */
  public static class DJInputFormatArray extends PairInputFormat<CellInfo, ArrayWritable> {
    @Override
    public RecordReader<PairWritableComparable<CellInfo>, PairWritable<ArrayWritable>> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      return new DJRecordReaderArray(job, (PairOfFileSplits)split);
    }
  }

  /**
   * Performs a redistribute join between the given files using the redistribute
   * join algorithm. Currently, we only support a pair of files.
   * @param fs
   * @param files
   * @param output
   * @return
   * @throws IOException 
   */
  public static long redistributeJoin(FileSystem fs, Path[] files,
      OutputCollector<PairShape<CellInfo>, PairShape<? extends Shape>> output) throws IOException {
    JobConf job = new JobConf(RedistributeJoin.class);
    
    Path outputPath;
    FileSystem outFs = files[0].getFileSystem(job);
    do {
      outputPath = new Path(files[0].toUri().getPath()+
          ".dj_"+(int)(Math.random() * 1000000));
    } while (outFs.exists(outputPath));
    outFs.deleteOnExit(outputPath);
    
    job.setJobName("RedistributeJoin");
    job.setMapperClass(RedistributeJoinMap.class);
    job.setMapOutputKeyClass(PairWritableComparable.class);
    job.setMapOutputValueClass(PairWritable.class);
    job.setNumReduceTasks(0); // No reduce needed for this task

    job.setInputFormat(DJInputFormatArray.class);
    job.set(SpatialSite.SHAPE_CLASS, TigerShape.class.getName());
    job.setOutputFormat(TextOutputFormat.class);
    
    String commaSeparatedFiles = "";
    for (int i = 0; i < files.length; i++) {
      if (i > 0)
        commaSeparatedFiles += ',';
      commaSeparatedFiles += files[i].toUri().toString();
    }
    DJInputFormatArray.addInputPaths(job, commaSeparatedFiles);
    TextOutputFormat.setOutputPath(job, outputPath);
    
    JobClient.runJob(job);

    // Read job result
    FileStatus[] results = outFs.listStatus(outputPath);
    long resultCount = 0;
    for (FileStatus fileStatus : results) {
      if (fileStatus.getLen() > 0 && fileStatus.getPath().getName().startsWith("part-")) {
        resultCount += RecordCount.recordCountLocal(outFs, fileStatus.getPath());
        if (output != null) {
          // Report every single result as a pair of shapes
          PairShape<CellInfo> cells =
              new PairShape<CellInfo>(new CellInfo(), new CellInfo());
          PairShape<? extends Shape> shapes =
              new PairShape<TigerShape>(new TigerShape(), new TigerShape());
          LineReader lineReader = new LineReader(outFs.open(fileStatus.getPath()));
          Text text = new Text();
          if (lineReader.readLine(text) > 0) {
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
    
    outFs.delete(outputPath, true);
    
    return resultCount;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] inputPaths = cla.getPaths();
    JobConf conf = new JobConf(RedistributeJoin.class);
    FileSystem fs = inputPaths[0].getFileSystem(conf);
    long t1 = System.currentTimeMillis();
    long resultSize = redistributeJoin(fs, inputPaths, null);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }
}
