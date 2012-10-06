package edu.umn.cs.spatialHadoop.operations;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.RTreeGridRecordWriter;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.CommandLineArguments;

/**
 * This performs a range query map reduce job with text file input.
 * @author aseldawy
 *
 */
public class RQMapReduce {
  public static final Log LOG = LogFactory.getLog(RQMapReduce.class);
  public static final String QUERY_SHAPE = "edu.umn.cs.spatial.mapReduce.RQMapReduce.QueryRectangle";
  public static Shape queryShape;

  public static class Map extends MapReduceBase implements
      Mapper<LongWritable, TigerShape, IntWritable, TigerShape> {
    
    private static final IntWritable ONE = new IntWritable(1);

    public void map(LongWritable shapeId, TigerShape tigerShape,
        OutputCollector<IntWritable, TigerShape> output, Reporter reporter)
        throws IOException {
      if (queryShape.isIntersected(tigerShape.shape)) {
        output.collect(ONE, tigerShape);
      }
    }
  }

  public static class RTreeMap extends MapReduceBase implements
      Mapper<CellInfo, RTree<TigerShape>, IntWritable, TigerShape> {
    private static final IntWritable ONE = new IntWritable(1);

    public void map(CellInfo cellInfo, RTree<TigerShape> rtree,
        final OutputCollector<IntWritable, TigerShape> output, Reporter reporter)
        throws IOException {
      Rectangle querymbr = queryShape.getMBR();
      RTree.ResultCollector<TigerShape> results = new RTree.ResultCollector<TigerShape>() {
        @Override
        public void add(TigerShape tigerShape) {
          try {
            output.collect(ONE, tigerShape);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      long t1 = System.currentTimeMillis();
      int numResults = rtree.search(querymbr, results);
    }
  }
  
  public static void localRQ(JobConf conf, Path inputFile, Path outputFile, Rectangle query) throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    long fileLength = fs.getFileStatus(inputFile).getLen();
    PrintStream output = new PrintStream(fs.create(outputFile));
    TigerShapeRecordReader reader = new TigerShapeRecordReader(conf, fs.open(inputFile), 0, fileLength);
    LongWritable key = reader.createKey();
    TigerShape value = reader.createValue();
    while (reader.next(key, value)) {
      if (value.isIntersected(query))
      {
        output.println(key+","+value);
      }
    }
    output.close();
  }

	/**
	 * Entry point to the file.
	 * Params rectangle:<query rectangle> <input filenames> <output filename>
	 * query rectangle: in the form x,y,width,height
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
  public static void main(String[] args) throws Exception {
    CommandLineArguments cla = new CommandLineArguments(args);
    JobConf conf = new JobConf(RQMapReduce.class);

    if (!FileSystem.get(conf).exists(cla.getInputPath()) &&
        FileSystem.getLocal(conf).exists(cla.getInputPath())) {
      conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
      // Run on a local file. No MapReduce
      localRQ(conf, cla.getInputPath(), cla.getOutputPath(), cla.getRectangle());
     return;
    }
    System.out.println("mapreduce.tasktracker.map.tasks.maximum: "+conf.get("mapreduce.tasktracker.map.tasks.maximum"));

    conf.setJobName("BasicRangeQuery");

    // Retrieve query rectangle and store it along with the job
    Rectangle queryRectangle = cla.getRectangle();
    // Set the rectangle to be used in map job
    conf.set(QUERY_SHAPE, queryRectangle.writeToString());
    // Define the subset to be processed of input file
    conf.set(SplitCalculator.QUERY_RANGE, queryRectangle.writeToString());

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(TigerShape.class);

    // Check whether input file is RTree or heap
    FileSystem fileSystem = cla.getInputPath().getFileSystem(conf);
    FSDataInputStream fileIn = fileSystem.open(cla.getInputPath());
    long fileMarker = fileIn.readLong();
    if (fileMarker == RTreeGridRecordWriter.RTreeFileMarker) {
      LOG.info("Searching RTree file");
      conf.setMapperClass(RTreeMap.class);
      conf.setInputFormat(RTreeInputFormat.class);
    } else {
      LOG.info("Searching regular file");
      conf.setMapperClass(Map.class);
      conf.setInputFormat(RQInputFormat.class);
    }
    fileIn.close();

    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
    conf.setOutputFormat(TextOutputFormat.class);

    // All files except last one are input files
    RQInputFormat.setInputPaths(conf, cla.getInputPaths());

    // Last argument is the output file
    FileOutputFormat.setOutputPath(conf, cla.getOutputPath());

    // Start the job
    JobClient.runJob(conf);
  }
}
