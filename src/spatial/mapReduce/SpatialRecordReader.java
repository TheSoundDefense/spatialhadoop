package spatial.mapReduce;
import java.io.IOException;
import java.io.InputStream;

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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import spatial.Rectangle;


public class SpatialRecordReader implements RecordReader<Rectangle, CollectionWritable<Rectangle>> {

	public static final String RECORD_LENGTH =
		"mapreduce.input.byterecordreader.record.length";

	private int recordSize;

	private long start;
	private long pos;
	private long end;

	private CompressionCodecFactory compressionCodecs = null;
	private InputStream in;
	private FSDataInputStream fileIn;
	private final Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;

	public SpatialRecordReader(JobConf job, FileSplit split) throws IOException {
	    this.recordSize = job.getInt(SpatialRecordReader.RECORD_LENGTH, Integer.MAX_VALUE);

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
	        in = cIn;
	        start = cIn.getAdjustedStart();
	        end = cIn.getAdjustedEnd();
	        filePosition = cIn; // take pos from compressed stream
	      } else {
	        in = codec.createInputStream(fileIn, decompressor);
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
	public boolean next(Rectangle key, CollectionWritable<Rectangle> value) throws IOException {
		// Clear all existing rectangle in value
		value.clear();
		
		// Prepare a buffer to read rectangle records
		byte[] buffer = new byte[recordSize];

		// Keep reading until can't read a full record
		while (getFilePosition() < end && in.read(buffer) == buffer.length) {
	    	int i = 0;
	    	while (i < buffer.length && buffer[i] == 0)
	    		i++;
	    	if (i == buffer.length) {
	    		// A null record indicates end of file
	    		// Return items read so far
	    		return value.size() > 0;
	    	}
	    	// Parse rectangle
	    	String rect = new String(buffer, "ASCII");
	    	String[] parts = rect.split(",");
	    	int id = Integer.parseInt(parts[0]);
	    	float x1 = Float.parseFloat(parts[1]);
	    	float y1 = Float.parseFloat(parts[2]);
	    	float x2 = Float.parseFloat(parts[3]);
	    	float y2 = Float.parseFloat(parts[4]);
	    	// Add to list of rectangles
	    	value.add(new Rectangle(id, x1, y1, x2, y2));
	        pos += buffer.length;
	    }
		// Return true if was able to read at least one rectangle
	    return value.size() > 0;
	}

	@Override
	public Rectangle createKey() {
		return new Rectangle();
	}


	@Override
	public CollectionWritable<Rectangle> createValue() {
		return new ArrayListWritable<Rectangle>();
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
