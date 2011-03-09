package edu.umn.cs;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;


public class ByteRecordReader implements RecordReader<LongWritable, BytesWritable> {

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

	public ByteRecordReader(JobConf job, FileSplit split) throws IOException {
	    this.recordSize = job.getInt(ByteRecordReader.RECORD_LENGTH, Integer.MAX_VALUE);

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
	public boolean next(LongWritable key, BytesWritable value) throws IOException {
	    while (getFilePosition() < end) {
	        key.set(pos);

	        byte[] data = new byte[recordSize];
	        int newSize = in.read(data);
	        value.setCapacity(newSize);
	        value.set(data, 0, newSize);
	        if (newSize == 0) {
	        	//No data to read
	        	return false;
	        }
	        pos += newSize;
	        return true;
	    }
	    return false;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}


	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
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
