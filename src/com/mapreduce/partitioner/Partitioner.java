package com.mapreduce.partitioner;

public interface Partitioner<Key> {
	int partition(Key key);
}
