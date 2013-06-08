package com.mapreduce.partitioner;


public class ConstPartitioner<Key> implements Partitioner<Key> {
	private final Integer val;
	
	public ConstPartitioner(int val) {
		this.val = val;
	}
	
	@Override
	public int partition(Key key) {
		return val;
	}
}
