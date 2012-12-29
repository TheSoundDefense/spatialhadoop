package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.Shape;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.PairShape;

/**
 * Performs a repartition join algorithm.
 * @author eldawy
 *
 */
public class RepartitionJoin {
  
  /**Logger for the class*/
  private static final Log LOG = LogFactory.getLog(RepartitionJoin.class);
  
  public static long repartitionJoin(FileSystem fs, Path[] files,
      Shape stockShape,
      OutputCollector<PairShape<CellInfo>, PairShape<? extends Shape>> output) throws IOException {
    long t1, t2;
    t1 = System.currentTimeMillis();
    // Determine which file to repartition (smaller file)
    Path smaller_file = null;
    Path larger_file = null;
    long smaller_size = Long.MAX_VALUE;
    long larger_size = Long.MIN_VALUE;
    for (Path file : files) {
      long size = fs.getFileStatus(file).getLen();
      // The equal is important to ensure that if we have two files of the
      // same exact size, one of them will be identified as the larger file
      // and the other is identified as the smaller file
      if (size <= smaller_size) {
        smaller_size = size;
        smaller_file = file;
      }
      if (size > larger_size) {
        larger_size = size;
        larger_file = file;
      }
    }
    
    // Get the partitions of the other file
    Set<CellInfo> cells = new HashSet<CellInfo>();
    
    long length = fs.getFileStatus(larger_file).getLen();
    
    BlockLocation[] fileBlockLocations =
        fs.getFileBlockLocations(fs.getFileStatus(larger_file), 0, length);
    for (BlockLocation blk : fileBlockLocations) {
      if (blk.getCellInfo() != null) {
        cells.add(blk.getCellInfo());
      }
    }
    
    // Repartition the smaller file
    Path partitioned_file;
    FileSystem outFs = smaller_file.getFileSystem(new Configuration());
    do {
      partitioned_file = new Path(smaller_file.toUri().getPath()+
          ".repartitioned_"+(int)(Math.random() * 1000000));
    } while (outFs.exists(partitioned_file));
    long blockSize = fs.getFileStatus(larger_file).getBlockSize();
    Repartition.repartitionMapReduce(smaller_file, partitioned_file,
        cells.toArray(new CellInfo[cells.size()]),
        blockSize, stockShape, false, false, true);
    t2 = System.currentTimeMillis();
    System.out.println("Repartition time "+(t2-t1)+" millis");
    
    if (!outFs.exists(partitioned_file)) {
      // This happens when the two files are disjoint
      return 0;
    }
    
    t1 = System.currentTimeMillis();
    // Redistribute join the larger file and the partitioned file
    long result_size = RedistributeJoin.redistributeJoin(fs,
        new Path[] {larger_file, partitioned_file}, stockShape, output);
    outFs.delete(partitioned_file, true);
    t2 = System.currentTimeMillis();
    System.out.println("Join time "+(t2-t1)+" millis");
    return result_size;
  }
  
  public static void main(String[] args) throws IOException {
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] inputPaths = cla.getPaths();
    JobConf conf = new JobConf(RedistributeJoin.class);
    FileSystem fs = inputPaths[0].getFileSystem(conf);
    Shape stockShape = cla.getShape(true);
    long t1 = System.currentTimeMillis();
    long resultSize = repartitionJoin(fs, inputPaths, stockShape, null);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
    System.out.println("Result size: "+resultSize);
  }
}
