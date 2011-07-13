package edu.umn.cs;

import org.apache.hadoop.fs.Path;

/**
 * Stores a range in a file.
 * @author aseldawy
 *
 */
public class FileRange {
  public final Path file;
  public final long start;
  public final long length;

  public FileRange(Path file, long start, long length) {
    super();
    this.file = file;
    this.start = start;
    this.length = length;
  }
}
