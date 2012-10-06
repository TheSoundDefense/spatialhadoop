package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.spatial.GridInfo;

import edu.umn.cs.CommandLineArguments;


/**
 * Performs a spatial join between two files. If both files are of the same grid,
 * SJRO is performed rightaway. If they are of different grids, the smaller file
 * is repartitioned according to the grid used in the larger file. After this,
 * SJRO is performed.
 * @author aseldawy
 *
 */
public class SJRepartitionMapReduce {
	public static final Log LOG = LogFactory.getLog(SJRepartitionMapReduce.class);

	public static void spatialJoinRepartition(JobConf conf, Path[] inputFiles, Path outputPath, boolean pack) throws IOException {
	  
    FileSystem outFS = FileSystem.get(conf);
    
    // Find grid info for largest file
    GridInfo gridInfo = SJMapReduce.calculateGridInfo(null, outFS, inputFiles);

    // Now, repartition all files that are not of this grid
    for (int i = 0; i < inputFiles.length; i++) {
      FileStatus fileStatus = outFS.getFileStatus(inputFiles[i]);
      if (fileStatus.getGridInfo() == null || !fileStatus.getGridInfo().equals(gridInfo)) {
        LOG.info("Going to repartition "+inputFiles[i]);
        Path repartitioned = new Path(inputFiles[i].toUri().getPath()+".grid");
        RepartitionMapReduce.repartition(conf, inputFiles[i], repartitioned, gridInfo, pack, false, true);
        // Use the repartitioned file instead of original file
        inputFiles[i] = repartitioned;
      }
    }
    
    // Currently, all files are of the same grid, go ahead to SpatialJoin
    SJROMapReduce.SJRO(conf, inputFiles, outputPath);
	}
	
	/**
	 * Entry point to the file.
	 * Params <input filenames> <output filename>
	 * input filenames: A list of paths to input files in HDFS
	 * output filename: A path to an output file in HDFS
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(RepartitionMapReduce.class);
    CommandLineArguments cla = new CommandLineArguments(args);
    Path[] inputFiles = cla.getInputPaths();
    Path outputPath = cla.getOutputPath();
    
    spatialJoinRepartition(conf, inputFiles, outputPath, cla.isPack());
	}
}
