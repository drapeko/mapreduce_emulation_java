package com.mapreduce.partitioner;


public class ModPartitioner<Key> implements Partitioner<Key> {

	private int numberOfReducers;
	
	public ModPartitioner(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
	
	@Override
	public int partition(Key key) {
		int hash = Math.abs(key.hashCode());
		return hash % numberOfReducers;
	}

}
