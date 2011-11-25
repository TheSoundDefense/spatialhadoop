package edu.umn.cs.spatialHadoop.mapReduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
      Mapper<LongWritable, TigerShape, LongWritable, TigerShape> {

    public void map(LongWritable shapeId, TigerShape tigerShape,
        OutputCollector<LongWritable, TigerShape> output, Reporter reporter)
        throws IOException {
      if (queryShape.isIntersected(tigerShape.shape)) {
        output.collect(shapeId, tigerShape);
      }
    }
  }

  public static class RTreeMap extends MapReduceBase implements
      Mapper<CellInfo, RTree<TigerShape>, LongWritable, TigerShape> {

    public void map(CellInfo cellInfo, RTree<TigerShape> rtree,
        final OutputCollector<LongWritable, TigerShape> output, Reporter reporter)
        throws IOException {
      Rectangle querymbr = queryShape.getMBR();
      RTree.ResultCollector<TigerShape> results = new RTree.ResultCollector<TigerShape>() {
        @Override
        public void add(TigerShape tigerShape) {
          try {
            output.collect(new LongWritable(tigerShape.id), tigerShape);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      long t1 = System.currentTimeMillis();
      int numResults = rtree.search(new long[] {querymbr.x, querymbr.y}, new long[] {querymbr.width, querymbr.height}, results);
      LOG.info("RTree disk search elapsed: "+(System.currentTimeMillis() - t1)+" millis to find "+numResults+" results");
    }
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
      JobConf conf = new JobConf(RQMapReduce.class);
      conf.setJobName("BasicRangeQuery");
      
      CommandLineArguments cla = new CommandLineArguments(args);
      
      // Retrieve query rectangle and store it along with the job
      Rectangle queryRectangle = cla.getRectangle();
      // Set the rectangle to be used in map job
      conf.set(QUERY_SHAPE, queryRectangle.writeToString());
      // Define the subset to be processed of input file
      conf.set(SplitCalculator.QUERY_RANGE, queryRectangle.writeToString());

      conf.setOutputKeyClass(LongWritable.class);
      conf.setOutputValueClass(TigerShape.class);

      // Check whether input file is RTree or heap
      FileSystem fileSystem = cla.getInputPath().getFileSystem(conf);
      FSDataInputStream fileIn = fileSystem.open(cla.getInputPath());
      int fileMarker = fileIn.readInt();
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
