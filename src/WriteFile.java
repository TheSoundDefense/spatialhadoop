import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {

	public static final String theFilename = "/hello.txt";
	private static final int BlockSize = 64 * 1024 * 1024;

	public static void main (String [] args) throws IOException {

     Configuration conf = new Configuration();
     conf.set("fs.default.name", "hdfs://localhost:9000");
     conf.set("dfs.data.dir", "/home/eldawy/hdfs/data");
     conf.set("dfs.name.dir", "/home/eldawy/hdfs/name");
     FileSystem fs = FileSystem.get(conf);

     Path filenamePath = new Path(theFilename);

     try {
       if (fs.exists(filenamePath)) {
         // remove the file first
         fs.delete(filenamePath, false);
       }

       FSDataOutputStream out = fs.create(filenamePath);
       for (int cy1 = 0; cy1 < 1024; cy1 += 256) {
    	   for (int cx1 = 0; cx1 < 1024; cx1 += 256) {
    		   int cx2 = cx1 + 256;
    		   int cy2 = cy1 + 256;
    		   long bytesSoFar = 0;
    		   
    		   LineNumberReader reader = new LineNumberReader(new FileReader("test.txt"));
    		   while (reader.ready()) {
    			   String line = reader.readLine();
    			   String[] parts = line.split(",");
    			   int px1 = Integer.parseInt(parts[1]);
    			   int py1 = Integer.parseInt(parts[2]);
    			   int px2 = Integer.parseInt(parts[3]);
    			   int py2 = Integer.parseInt(parts[4]);
    			   if (!(px1 > cx2 || px2 < cx1)) {
    				   if (!(py1 > cy2 || py2 < cy1)) {
    					   // This rectangle belongs to this cell and should be written
    					   out.writeUTF(line);
    					   bytesSoFar += line.getBytes("utf-8").length + 2;
    				   }
    			   }
    		   }
    		   reader.close();
    		   
    		   // Complete this block with zeros
    		   long remainingBytes = BlockSize - bytesSoFar % BlockSize;
    		   while (remainingBytes-- > 0) {
    			   out.writeByte(0);
    		   }
    	   }
    	   
       }
       out.close();

       FSDataInputStream in = fs.open(filenamePath);
       String messageIn = in.readUTF();
       System.out.print(messageIn);
       in.close();
     } catch (IOException ioe) {
       System.err.println("IOException during operation: " + ioe.toString());
       System.exit(1);
     }
   }
}