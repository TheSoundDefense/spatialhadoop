package edu.umn.cs.spatial.mapReduce;
import java.io.DataInputStream;
import java.io.IOException;

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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.edu.spatial.Point;
import edu.umn.edu.spatial.Rectangle;


/**
 * Parses spatial file records.
 * @author aseldawy
 *
 */
public class PointRecordReader implements RecordReader<Rectangle, Point> {

	private long start;
	private long pos;
	private long end;

	private CompressionCodecFactory compressionCodecs = null;
	private DataInputStream in;
	private FSDataInputStream fileIn;
	private final Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;

	public PointRecordReader(FileSplit split, Configuration job, Reporter reporter) throws IOException {
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

	@Override
	public boolean next(Rectangle key, Point value) throws IOException {
		if (getFilePosition() >= end)
			return false;
		
    	// Read a rectangle
    	int id = in.readInt();
    	pos += 4;
    	if (id == 0) {
    		// An item with zero ID indicates end of file
    		// Return items read so far
    		// When no items are read, this indicates that this split is finished
    		return false;
    	}
    	float x = in.readFloat();
    	float y = in.readFloat();
    	pos += 2 * 4;
    	value.x = x;
    	value.y = y;
		// Return true if was able to read at least one rectangle
	    return true;
	}

	@Override
	public Rectangle createKey() {
		return new Rectangle();
	}


	@Override
	public Point createValue() {
		return new Point();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
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

	@Override
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
