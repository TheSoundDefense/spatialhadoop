package edu.umn.cs.spatial.mapReduce;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import edu.umn.edu.spatial.Rectangle;


/**
 * Parses spatial file records.
 * @author aseldawy
 *
 */
public class RectanglesRecordReader implements RecordReader<Rectangle, CollectionWritable<Rectangle>> {

	private long start;
	private long pos;
	private long end;

	private CompressionCodecFactory compressionCodecs = null;
	private DataInputStream in;
	private FSDataInputStream fileIn;
	private final Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;

	public RectanglesRecordReader(CombineFileSplit split, Configuration job, Reporter reporter, Integer idx) throws IOException {
	    start = split.getOffset(idx);
	    end = start + split.getLength(idx);
	    final Path file = split.getPath(idx);
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

	public boolean next(Rectangle key, CollectionWritable<Rectangle> value) throws IOException {
		// Clear all existing rectangle in value
		value.clear();
		
		// Keep reading until can't read a full record
		while (getFilePosition() < end) {
	    	// Read a rectangle
	    	int id = in.readInt();
	    	pos += 4;
	    	if (id == 0) {
	    		// An item with zero ID indicates end of file
	    		// Return items read so far
	    		// When no items are read, this indicates that this split is finished
	    		return value.size() > 0;
	    	}
	    	float x1 = in.readFloat();
	    	float y1 = in.readFloat();
	    	float x2 = in.readFloat();
	    	float y2 = in.readFloat();
	    	pos += 4 * 4;
	    	// Add to list of rectangles
	    	value.add(new Rectangle(id, x1, y1, x2, y2));
	    }
		// Return true if was able to read at least one rectangle
	    return value.size() > 0;
	}

	public Rectangle createKey() {
		return new Rectangle();
	}

	public CollectionWritable<Rectangle> createValue() {
		return new ArrayListWritable<Rectangle>();
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
