import java.awt.Rectangle;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {

	public static final String theFilename = "/hello.dat";
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
    		   Rectangle cell = new Rectangle(cx1, cy1, cx1 + 256, cy1 + 256);
    		   long bytesSoFar = 0;
    		   
    		   LineNumberReader reader = new LineNumberReader(new FileReader("test.txt"));
    		   while (reader.ready()) {
    			   String line = reader.readLine();
    			   String[] parts = line.split(",");
    			   int px1 = Integer.parseInt(parts[1]);
    			   int py1 = Integer.parseInt(parts[2]);
    			   int px2 = Integer.parseInt(parts[3]);
    			   int py2 = Integer.parseInt(parts[4]);
    			   Rectangle r = new Rectangle(px1, py1, px2, py2);
    			   if (r.intersects(cell)) {
    				   // This rectangle belongs to this cell and should be written
    				   byte[] data = line.getBytes("ASCII");
    				   // Write leading zeros to be sure block size is 32
    				   for (int i = 0; i < (32 - data.length); i++) {
    					   out.writeByte('0');
    					   bytesSoFar++;
    				   }
    				   out.write(data);
    				   bytesSoFar += data.length;
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

       /*FSDataInputStream in = fs.open(filenamePath);
       String messageIn = in.readUTF();
       System.out.print(messageIn);
       in.close();*/
     } catch (IOException ioe) {
       System.err.println("IOException during operation: " + ioe.toString());
       System.exit(1);
     }
   }
}