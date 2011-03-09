package edu.umn.cs;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {

	/**Size of one block in bytes*/
	private static final int BlockSize = 64 * 1024 * 1024;
	/**Grid dimensions*/
	private static final double GridX1 = 0;
	private static final double GridY1 = 0;
	private static final double GridX2 = 1024;
	private static final double GridY2 = 1024;
	/**Cell width*/
	private static final double CellWidth = 512;
	/**Cell height*/
	private static final double CellHeight = 512;
	
	/**Number of grid cell columns*/
	private static final int GridColumns = (int)Math.ceil((GridX2 - GridX1) / CellWidth);
	/**Number of grid cell rows*/
	private static final int GridRows = (int)Math.ceil((GridY2 - GridY1) / CellHeight);

	public static void main (String [] args) throws IOException {
		String inputFilename = args[0];
		String outputFilename = args[1];
		String histogramFilename = outputFilename + ".hist";

		// Initialize histogram
		int [][]histogram = new int [GridColumns][GridRows];

		// Set default server configuration
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		//conf.set("dfs.data.dir", "/home/khalefa/hadoop-khalefa/dfs/data");
		//conf.set("dfs.name.dir", "/home/khalefa/hadoop-khalefa/dfs/name");
		
		// Get the HDFS file system
		FileSystem fs = FileSystem.get(conf);

		// Delete output file if already exists
		Path outputFilepath = new Path(outputFilename);

		if (fs.exists(outputFilepath)) {
			// remove the file first
			fs.delete(outputFilepath, false);
		}

		// Open an output stream for the file
		FSDataOutputStream out = fs.create(outputFilepath, GridX1, GridY1, GridX2, GridY2, CellWidth, CellHeight);

		// Run the loop for every grid cell
		for (int cy1 = 0; cy1 < GridX2; cy1 += CellWidth) {
			for (int cx1 = 0; cx1 < GridY2; cx1 += CellHeight) {
				double cx2 = cx1 + CellWidth;
				double cy2 = cy1 + CellHeight;
				long bytesSoFar = 0;

				LineNumberReader reader = new LineNumberReader(new FileReader(inputFilename));
				while (reader.ready()) {
					String line = reader.readLine();
					// Parse rectangle dimensions
					String[] parts = line.split(",");
					int id = Integer.parseInt(parts[0]);
					float rx1 = Float.parseFloat(parts[1]);
					float ry1 = Float.parseFloat(parts[2]);
					float rx2 = Float.parseFloat(parts[3]);
					float ry2 = Float.parseFloat(parts[4]);

					if (!(rx1 > cx2 || rx2 < cx1)) {
						if (!(ry1 > cy2 || ry2 < cy1)) {
							// This rectangle belongs to this cell and should be written
							int x_i = (int)Math.round(cx1 / CellWidth);
							int y_i = (int)Math.round(cy1 / CellHeight);
							histogram[x_i][y_i]++;
							// Write ID, x1, y1, x2, y2
							out.writeInt(id);
							out.writeFloat(rx1);
							out.writeFloat(ry1);
							out.writeFloat(rx2);
							out.writeFloat(ry2);
							bytesSoFar += 4;
							bytesSoFar += 4 * 4;
						}
					}
				}
				reader.close();

				// Complete this block with zeros
				// We use % because we might have written multiple blocks
				// for this grid cell
				long remainingBytes = BlockSize - bytesSoFar % BlockSize;
				while (remainingBytes-- > 0) {
					out.writeByte(0);
				}
			}

		}
		out.close();

		// Write a file for the histogram
		Path histFilepath = new Path(histogramFilename);
		if (fs.exists(histFilepath)) {
			// remove the file first
			fs.delete(histFilepath, false);
		}

		FSDataOutputStream os = fs.create(histFilepath);

		for (int i = 0; i < GridColumns; i++) {
			for (int j = 0; j < GridRows; j++) {
				System.out.print(histogram[i][j] + " ");
				os.writeInt(histogram[i][j]);
			}
			System.out.print("\n");
		}
		os.close();
	}
}