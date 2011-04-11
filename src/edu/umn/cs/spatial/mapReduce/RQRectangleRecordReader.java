package edu.umn.cs.spatial.mapReduce;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.edu.spatial.Rectangle;


/**
 * Parses a spatial file and returns a list of <id, rectangle> tuples.
 * Used with range query (spatial selection)
 * @author aseldawy
 *
 */
public class RQRectangleRecordReader implements RecordReader<IntWritable, Rectangle> {

	private long start;
	private long pos;
	private long end;

	private CompressionCodecFactory compressionCodecs = null;
	private DataInputStream in;
	private FSDataInputStream fileIn;
	private final Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;

	public RQRectangleRecordReader(FileSplit split, Configuration job, Reporter reporter) throws IOException {
	    start = split.getStart();
	    end = start + split.getLength();
	    final Path file = split.getPath();
	    compressionCodecs = new CompressionCodecFactory(job);
	    codec = compressionCodecs.getCodec(file);

	    // open the file and seek to the start of the split
	    final FileSystem fs = file.getFileSystem(job);
	    fileIn = fs.open(file);
	    if (isCompressedInput()) {
	      decompressor = CodecPool.getDecompressor(codec);
	      if (codec instanceof SplittableCompressionCodec) {
	        final SplitCompressionInputStream cIn =
	          ((SplittableCompressionCodec)codec).createInputStream(
	            fileIn, decompressor, start, end,
	            SplittableCompressionCodec.READ_MODE.BYBLOCK);
	        in = new DataInputStream(cIn);
	        start = cIn.getAdjustedStart();
	        end = cIn.getAdjustedEnd();
	        filePosition = cIn; // take pos from compressed stream
	      } else {
	        in = new DataInputStream(codec.createInputStream(fileIn, decompressor));
	        filePosition = fileIn;
	      }
	    } else {
	      fileIn.seek(start);
	      in = fileIn;
	      filePosition = fileIn;
	    }
	    this.pos = start;
	
	}

	private boolean isCompressedInput() {
		return (codec != null);
	}

	/**
	 * Reads a rectangle and emits it.
	 * It skips bytes until end of line is found.
	 * After this, it starts reading rectangles with a line in each rectangle.
	 * It consumes the end of line also when reading the rectangle.
	 * It stops after reading the first end of line (after) end.
	 */
	public boolean next(IntWritable key, Rectangle value) throws IOException {
		if (getFilePosition() > end || in.available() == 0)
			return false;

		if (getFilePosition() == start && getFilePosition() > 0) {
			// Special treatment for first record in each split
			// Since first record is most probably spanned across two splits,
			// it is skipped in this split and the split before me should read it.
			// The only exception is the first split because there is no prior split.
			char c;
			do {
				c = in.readChar();
		    	pos++;
			} while (in.available() > 0 && c != '\n' && c != '\r');
		}

		if (getFilePosition() > end || in.available() == 0)
			return false;

		// Read a line assuming a maximum length bytes per line
		String line = "";
		
    	byte[] lineBytes = new byte[4096];
    	// Read until end of line is found
    	int i = -1;
    	do {
    		i++;
    		if (i == lineBytes.length) {
    			// Append bytes written so far. This assumes ASCII format of string.
    			line += new String(lineBytes);
    			// Reset buffer
    			i = 0;
    		}
    		lineBytes[i] = in.readByte();
    		pos++;
    	} while (in.available() > 0 && lineBytes[i] != '\n' && lineBytes[i] != '\r');
    	
    	// Append last read chunk
    	line += new String(lineBytes, 0, i+1);

    	String[] parts = line.split(",");
    	key.set(Integer.parseInt(parts[0]));
    	value.id = key.get();
    	value.x1 = Float.parseFloat(parts[1]);
    	value.y1 = Float.parseFloat(parts[2]);
    	value.x2 = Float.parseFloat(parts[3]);
    	value.y2 = Float.parseFloat(parts[4]);
      // Return true if was able to read at least one rectangle
	    return true;
	}

	public IntWritable createKey() {
		return new IntWritable();
	}

	public Rectangle createValue() {
		return new Rectangle();
	}

	public long getPos() throws IOException {
		return pos;
	}

	public void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
	}

	public float getProgress() throws IOException {
	    if (start == end) {
	        return 0.0f;
	      } else {
	        return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
	      }
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

}
