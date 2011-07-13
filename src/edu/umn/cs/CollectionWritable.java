package edu.umn.cs;

import java.util.Collection;

import org.apache.hadoop.io.Writable;

public interface CollectionWritable<E extends Writable> extends Collection<E>, Writable {

}
