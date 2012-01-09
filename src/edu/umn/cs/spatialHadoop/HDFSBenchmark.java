package edu.umn.cs.spatialHadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSBenchmark {

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
    String command = args[0];
    Path path = new Path(args[1]);
    String readMode = "sequential";
    String writeMode = "oneshot";
    int bufferSize = 4096;
    int chunkSize = 1024*1024;
    long t1=0, t2=0; // start and end time
    long totalSize = 0; // Total bytes read/written to disk
    for (int i = 2; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--sequential")) {
        readMode = "sequential";
      } else if (arg.equals("--randomRead")) {
        readMode = "random";
        chunkSize = Integer.parseInt(args[++i]);
      } else if (arg.equals("--append")) {
        writeMode = "append";
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
    if (command.equals("write"))
      System.out.println(writeMode);
    else
      System.out.println(readMode);
    System.out.println("BufferSize: "+bufferSize);
    System.out.println("ChunkSize: "+chunkSize);
    
    if (command.equals("write")) {
      // write a sequential file to HDFS
      if (writeMode.equals("oneshot")) {
        t1 = System.currentTimeMillis();
        OutputStream os = hdfs.create(path, true, bufferSize);
        for (int i = 0; i < 1000; i++) {
          os.write(chunk);
          totalSize += chunk.length;
        }
        os.close();
        t2 = System.currentTimeMillis();
      }
      if (writeMode.equals("append")) {
        t1 = System.currentTimeMillis();
        // Create the file
        hdfs.create(path, true).close();
        for (int i = 0; i < 1000; i++) {
          OutputStream os = hdfs.append(path, bufferSize);
          os.write(chunk);
          totalSize += chunk.length;
          os.close();
        }
        t2 = System.currentTimeMillis();
      }
    } else if (command.equals("read")) {
      // Read a file sequentially
      if (readMode.equals("sequential")) {
        t1 = System.currentTimeMillis();
        InputStream is = hdfs.open(path, bufferSize);
        int readSize;
        do {
          readSize = is.read(chunk);
          totalSize += readSize;
        } while (readSize > 0);
        is.close();
        t2 = System.currentTimeMillis();
      } else if (readMode.equals("random")) {
        // Read random data from file using default buffer size
        t1 = System.currentTimeMillis();
        long fileSize = hdfs.getFileStatus(path).getLen();
        FSDataInputStream is = hdfs.open(path, bufferSize);
        int readSize;
        for (int i = 0; i < 1000; i++) {
          long randomPosition = (long) (Math.random() * fileSize);
          is.seek(randomPosition);
          readSize = is.read(chunk);
          totalSize += readSize;
        }
        is.close();
        t2 = System.currentTimeMillis();
      }
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
    System.out.println("command: Either read or write");
    System.out.println("filename an HDFS path for a file to read or write");
    System.out.println("options:");
    System.out.println(" --bufferSize <size>");
    System.out.println(" --sequential read the whole file sequentially");
    System.out.println(" --randomRead <chunk size> read random parts of the file");
    System.out.println(" --append <chunk size> for write");
  }

}
