package edu.umn.cs.spatialHadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.spatial.Shape;
import org.apache.hadoop.spatial.TigerShape;
import org.apache.hadoop.util.LineReader;

public class HDFSBenchmark {

  enum Command {WRITE, READ, PROCESS};
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
    Configuration hdfsConfig = new Configuration();
    FileSystem hdfs = FileSystem.get(hdfsConfig);
    Command command = null;
    Path path = new Path(args[1]);
    ReadMode readMode = ReadMode.SEQUENTIAL;
    WriteMode writeMode = WriteMode.ONESHOT;
    String shapestr = "Rectangle";
    int bufferSize = 4096;
    int chunkSize = 1024*1024;
    long t1=0, t2=0; // start and end time
    long totalSize = 0; // Total bytes read/written to disk
    if (args[0].equals("read")) {
      command = Command.READ;
    } else if (args[0].equals("write")) {
      command = Command.WRITE;
    } else if (args[0].equals("process")) {
      command = Command.PROCESS;
    }
    for (int i = 2; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--sequential")) {
        readMode = ReadMode.SEQUENTIAL;
      } else if (arg.equals("--randomRead")) {
        readMode = ReadMode.RANDOM;
        chunkSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--append")) {
        writeMode = WriteMode.APPEND;
        chunkSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--local")) {
        hdfs = FileSystem.getLocal(hdfsConfig);
      } else {
        System.err.println("Invalid option: "+arg);
        System.exit(1);
      }
    }
    byte[] chunk = new byte[chunkSize];
    // Print what we are going to do
    System.out.print(command+" "+path+" ");
    if (command == Command.WRITE)
      System.out.println(writeMode);
    else if (command == Command.READ)
      System.out.println(readMode);
    System.out.println("BufferSize: "+bufferSize);
    System.out.println("ChunkSize: "+chunkSize);
    
    OutputStream os;
    switch (command) {
    case WRITE:
      switch (writeMode) {
      case ONESHOT:
        // write a sequential file to HDFS
        t1 = System.currentTimeMillis();
        os = hdfs.create(path, true, bufferSize);
        for (int i = 0; i < 1000; i++) {
          os.write(chunk);
          totalSize += chunk.length;
        }
        os.close();
        t2 = System.currentTimeMillis();
        break; // ONESHOT
      case APPEND:
        t1 = System.currentTimeMillis();
        // Create the file
        hdfs.create(path, true).close();
        for (int i = 0; i < 1000; i++) {
          os = hdfs.append(path, bufferSize);
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
        InputStream is = hdfs.open(path, bufferSize);
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
        long fileSize = hdfs.getFileStatus(path).getLen();
        FSDataInputStream fsdis = hdfs.open(path, bufferSize);
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
        LineReader lr = new LineReader(hdfs.open(path, bufferSize));
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
    }

    // Write results
    System.out.println("Total size: "+totalSize+" bytes");
    long totalTime = t2 - t1;
    System.out.println("Total time: "+totalTime+" millis");
    double throughput = (double) totalSize * 1000 / totalTime / 1024 / 1024;
    System.out.println("Throughput: "+throughput+" MBytes/sec");
  }

  private static void printUsage() {
    System.out.println("Calculate HDFS I/O throughput in MB/sec");
    System.out.println("Usage: <command> <filename> [options]");
    System.out.println("command: Either read, write or process");
    System.out.println("filename an HDFS path for a file to read or write");
    System.out.println("options:");
    System.out.println(" --bufferSize <size>");
    System.out.println(" --sequential read the whole file sequentially");
    System.out.println(" --randomRead <chunk size> read random parts of the file");
    System.out.println(" --append <chunk size> for write");
    System.out.println(" --shape <shape name> to process");
  }

}
