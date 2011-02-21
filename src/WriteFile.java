import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {
	public static  String  HIST_FILENAME = "histgram.txt";
    public static int [][]histogram;
     
	public static final String theFilename = "/hello.txt";
	private static final int BlockSize = 64 * 1024 * 1024;
	private static final int X= 1024;
	private static final int Y= 1024;
	private static final int GRID_X = 128;
	private static final int GRID_Y = 128;
	private static final int N_X = X/GRID_X;
	private static final int N_Y = Y/GRID_Y;
	
	public static void main (String [] args) throws IOException {
		
		histogram = new int [N_X][N_Y];
		for(int i=0;i<N_X;i++){
			for(int j=0;j<N_Y;j++){
				histogram[i][j] = 0;
		     }   
	     }
     Configuration conf = new Configuration();
     conf.set("fs.default.name", "hdfs://localhost:54310");
     conf.set("dfs.data.dir", "/home/khalefa/hadoop-khalefa/dfs/data");
     conf.set("dfs.name.dir", "/home/khalefa/hadoop-khalefa/dfs/name");
     FileSystem fs = FileSystem.get(conf);

     Path filenamePath = new Path(theFilename);

     try {
       if (fs.exists(filenamePath)) {
         // remove the file first
         fs.delete(filenamePath, false);
       }

       FSDataOutputStream out = fs.create(filenamePath);
       for (int cy1 = 0; cy1 < X; cy1 += GRID_X) {
    	   for (int cx1 = 0; cx1 < Y; cx1 += GRID_Y) {
    		   int cx2 = cx1 + GRID_X;
    		   int cy2 = cy1 + GRID_Y;
    		   long bytesSoFar = 0;
    		   
    		   LineNumberReader reader = new LineNumberReader(new FileReader("/home/khalefa/Desktop/test.txt"));
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
    					   int x_i = cx1/GRID_X;
    					   int y_i = cy1/GRID_Y;
    					   histogram[x_i][y_i]++;        
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
       System.out.print("\n");
       System.out.print("\n");
       DataOutputStream os = new DataOutputStream(new FileOutputStream(HIST_FILENAME));
       for(int i=0;i< N_X; i++) {
    	   for(int j=0;j< N_Y; j++){
    		System.out.print(histogram[i][j]+" ");   
             os.writeInt(histogram[i][j]);
    	   }
    	   System.out.print("\n");
    	   }
       os.close();
       
     } catch (IOException ioe) {
       System.err.println("IOException during operation: " + ioe.toString());
       System.exit(1);
     }
   }
}