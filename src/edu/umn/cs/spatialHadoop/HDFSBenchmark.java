package edu.umn.cs.spatialHadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.spatial.Rectangle;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.mapReduce.RQInputFormat;
import edu.umn.cs.spatialHadoop.mapReduce.RQMapReduce;
import edu.umn.cs.spatialHadoop.mapReduce.SplitCalculator;
import edu.umn.cs.spatialHadoop.mapReduce.TigerShapeRecordReader;

public class HDFSBenchmark {

  enum Command {WRITE, READ, PROCESS, NULL_MAPREDUCE};
  enum ReadMode {SEQUENTIAL, RANDOM};
  enum WriteMode {ONESHOT, APPEND};

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      printUsage();
      return;
    }
    Configuration conf = new Configuration();
    FileSystem fileSystem = null;
    Command command = null;
    Path path = new Path(args[1]);
    ReadMode readMode = ReadMode.SEQUENTIAL;
    WriteMode writeMode = WriteMode.ONESHOT;
    String shapestr = "Rectangle";
    int bufferSize = 4096;
    int chunkSize = 1024*1024;
    long totalWriteSize = 0;
    
    long t1=0, t2=0; // start and end time
    long totalSize = 0; // Total bytes read/written to disk
    String strCommand = args[0].toLowerCase();
    if (strCommand.equals("read")) {
      command = Command.READ;
    } else if (strCommand.equals("write")) {
      command = Command.WRITE;
    } else if (strCommand.equals("process")) {
      command = Command.PROCESS;
    } else if (strCommand.equals("mapreduce")) {
      command = Command.NULL_MAPREDUCE;
    } else {
      throw new RuntimeException("Unknown command: "+strCommand);
    }
    for (int i = 2; i < args.length; i++) {
      String arg = args[i].toLowerCase();
      if (arg.equals("--bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--sequential")) {
        readMode = ReadMode.SEQUENTIAL;
      } else if (arg.equals("--randomread")) {
        readMode = ReadMode.RANDOM;
        chunkSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--append")) {
        writeMode = WriteMode.APPEND;
        chunkSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--totalsize")) {
        totalWriteSize = parseSize(args[++i]);
      } else if (arg.equals("--local")) {
        fileSystem = FileSystem.getLocal(conf);
      } else if (arg.equals("--hdfs") || arg.equals("--remote")) {
        fileSystem = FileSystem.get(conf);
      } else {
        throw new RuntimeException("Invalid option: "+arg);
      }
    }
    
    if (fileSystem == null) {
      // Auto detect file system
      // TODO move to a separate function
      switch (command) {
      case READ:
      case PROCESS:
        // Auto detect file system
        fileSystem = FileSystem.getLocal(conf);
        if (!fileSystem.exists(path)) {
          fileSystem = FileSystem.get(conf);
          if (!fileSystem.exists(path)) {
            throw new RuntimeException("Path: '"+path+"' not found on local or remote (HDFS) file system");
          }
        }
        break;
      case WRITE:
      case NULL_MAPREDUCE:
        fileSystem = FileSystem.get(conf);
        break;
      }
    }
    byte[] chunk = new byte[chunkSize];
    // Print what we are going to do
    System.out.print(command+" "+path+" ");
    if (fileSystem instanceof LocalFileSystem)
      System.out.print("(local) ");
    else if (fileSystem instanceof DistributedFileSystem)
      System.out.print("(HDFS) ");
    if (command == Command.WRITE)
      System.out.println(writeMode);
    else if (command == Command.READ)
      System.out.println(readMode);
    System.out.println("BufferSize: "+bufferSize);
    System.out.println("ChunkSize: "+chunkSize);
    
    OutputStream os;
    switch (command) {
    case WRITE:
      if (totalWriteSize == 0)
        totalWriteSize = chunkSize * 1024;
      // Fill buffer with new lines
      Arrays.fill(chunk, (byte)'\n');
      switch (writeMode) {
      case ONESHOT:
        // write a sequential file to HDFS
        t1 = System.currentTimeMillis();
        os = fileSystem.create(path, true, bufferSize);
        while (totalSize < totalWriteSize) {
          os.write(chunk);
          totalSize += chunk.length;
        }
        os.close();
        t2 = System.currentTimeMillis();
        break; // ONESHOT
      case APPEND:
        t1 = System.currentTimeMillis();
        // Create the file
        fileSystem.create(path, true).close();
        while (totalSize < totalWriteSize) {
          os = fileSystem.append(path, bufferSize);
          os.write(chunk);
          totalSize += chunk.length;
          os.close();
        }
        t2 = System.currentTimeMillis();
        break; // APPEND
      }
      break; // WRITE
    case READ:
      switch (readMode) {
      case SEQUENTIAL:        
        // Read a file sequentially
        t1 = System.currentTimeMillis();
        InputStream is = fileSystem.open(path, bufferSize);
        for (int i = 0; i < 1000; i++) {
          int readSize = is.read(chunk);
          totalSize += readSize;
          if (readSize == 0)
            break;
        };
        is.close();
        t2 = System.currentTimeMillis();
        break; // SEQUENTIAL
      case RANDOM:
        // Read random data from file using default buffer size
        t1 = System.currentTimeMillis();
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FSDataInputStream fsdis = fileSystem.open(path, bufferSize);
        int readSize;
        for (int i = 0; i < 1000; i++) {
          long randomPosition = (long) (Math.random() * fileSize);
          fsdis.seek(randomPosition);
          readSize = fsdis.read(chunk);
          totalSize += readSize;
        }
        fsdis.close();
        t2 = System.currentTimeMillis();
        break; // RANDOM
      }
      break; // READ
    case PROCESS:
      try {
        // Read a file sequentially
        String shapeClassName = Shape.class.getName().replace("Shape", shapestr);
        Class<Shape> shapeClass = (Class<Shape>) Class.forName(shapeClassName);
        TigerShape shape;
        shape = new TigerShape(shapeClass.newInstance(), 0);
        Text line = new Text();
        t1 = System.currentTimeMillis();
        LineReader lr = new LineReader(fileSystem.open(path, bufferSize));
        for (int i = 0; i < 5000000; i++) {
          int readSize = lr.readLine(line);
          if (readSize == 0)
            break;
          shape.fromText(line);
          totalSize += readSize;
        };
        lr.close();
        t2 = System.currentTimeMillis();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      break; // PROCESS
      
      case NULL_MAPREDUCE:
        if (fileSystem instanceof LocalFileSystem)
          throw new RuntimeException("MapReduce can be used only with remote files");
        totalSize = fileSystem.getFileStatus(path).getLen();
        t1 = System.currentTimeMillis();
        runNullMapReduceJob(fileSystem, path, bufferSize);
        t2 = System.currentTimeMillis();
      break; // NULL_MAPREDUCE
    }

    // Write results
    System.out.println("Total size: "+totalSize+" bytes");
    long totalTime = t2 - t1;
    System.out.println("Total time: "+totalTime+" millis");
    double throughput = (double) totalSize * 1000 / totalTime / 1024 / 1024;
    System.out.println("Throughput: "+throughput+" MBytes/sec");
  }

  private static Pattern SizePattern = Pattern.compile("(\\d+|\\w+)");
  
  private static long parseSize(String encoded) {
    long parsed_size = 1;
    encoded = encoded.toLowerCase();
    Matcher m = SizePattern.matcher(encoded);
    
    while (m.find()) {
      String part = m.group(0);
      if (part.matches("\\d+")) {
        parsed_size *= Long.parseLong(part);
      } else if (part.startsWith("k")) {
        parsed_size *= 1024;
      } else if (part.startsWith("m")) {
        parsed_size *= 1024 * 1024;
      } else if (part.startsWith("g")) {
        parsed_size *= 1024 * 1024 * 1024;
      }
    }
    return parsed_size;
  }

  private static void runNullMapReduceJob(FileSystem hdfs, Path path,
      int bufferSize) throws IOException {
    JobConf conf = new JobConf(HDFSBenchmark.class);
    conf.setJobName("NullMapReduce");
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(TigerShape.class);

    // Set a dummy query rectangle for RQInputFormat to work
    Rectangle queryRectangle = new Rectangle();
    conf.set(RQMapReduce.QUERY_SHAPE, queryRectangle.writeToString());
    conf.set(SplitCalculator.QUERY_RANGE, queryRectangle.writeToString());
    
    conf.set(TigerShapeRecordReader.SHAPE_CLASS, Rectangle.class.getName());
    conf.setMapperClass(RQMapReduce.Map.class);
    conf.setInputFormat(RQInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    RQInputFormat.addInputPath(conf, path);
    Path outPath = new Path("/null_out");
    hdfs.delete(outPath, true);
    FileOutputFormat.setOutputPath(conf, outPath);
    
    JobClient.runJob(conf);
  }

  private static void printUsage() {
    System.out.println("Calculate HDFS I/O throughput in MB/sec");
    System.out.println("Usage: <command> <filename> [options]");
    System.out.println("command: Either read, write, process or mapreduce");
    System.out.println("filename an HDFS path for a file to read or write");
    System.out.println("options:");
    System.out.println(" --bufferSize <size>");
    System.out.println(" --sequential read the whole file sequentially");
    System.out.println(" --randomRead <chunk size> read random parts of the file");
    System.out.println(" --append <chunk size> for write");
    System.out.println(" --totalSize <total file size> for write");
    System.out.println(" --local use local file system");
    System.out.println(" --hdfs --remote use remote or (HDFS) file system (both are equivalent)");
    System.out.println(" --shape <shape name> to process");
  }

}
