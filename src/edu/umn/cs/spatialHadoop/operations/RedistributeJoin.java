package edu.umn.cs.spatialHadoop.operations;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.spatial.CellInfo;
import org.apache.hadoop.spatial.RTree;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;

import edu.umn.cs.CommandLineArguments;
import edu.umn.cs.spatialHadoop.mapReduce.STextOutputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.ShapeRecordReader;
import edu.umn.cs.spatialHadoop.mapReduce.SplitCalculator;

/**
 * Performs a spatial join between two or more files using the redistribute-join
 * algorithm.
 * @author eldawy
 *
 */
public class RedistributeJoin {

  /**The map function for the redistribute join*/
  public static class Map extends MapReduceBase
  implements Mapper<CellInfo, Pair<RTree<Shape>>, CellInfo, Pair<Shape>> {
    @Override
    public void map(CellInfo key, Pair<RTree<Shape>> value,
        OutputCollector<CellInfo, Pair<Shape>> output, Reporter reporter)
        throws IOException {
      // TODO Auto-generated method stub
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
  public static int redistributeJoin(FileSystem fs, Path[] files,
      OutputCollector<CellInfo, Pair<Shape>> output) throws IOException {
    JobConf job = new JobConf(RedistributeJoin.class);
    
    Path outputPath =
        new Path(files[0].toUri().getPath()+".dj_"+Math.random()*10000);
    FileSystem outFs = outputPath.getFileSystem(job);
    outFs.delete(outputPath, true);
    
    job.setJobName("RedistributeJoin");
    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(CellInfo.class);
    job.setMapOutputValueClass(Pair.class);

    job.setInputFormat(ShapeInputFormat.class);
    job.set(ShapeRecordReader.SHAPE_CLASS, TigerShape.class.getName());
    String query_point_distance = queryPoint.x+","+queryPoint.y+","+0;
    job.set(SplitCalculator.QUERY_POINT_DISTANCE, query_point_distance);
    job.setOutputFormat(STextOutputFormat.class);

    return 0;
  }
  
  public static void main(String[] args) {
    CommandLineArguments cla = new CommandLineArguments(args);
    
  }
}
