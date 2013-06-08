package com.mapreduce.collector;

import java.io.BufferedWriter;
import java.util.List;


public interface OutputCollector <K, V> {
	
	void collect(K key, V value);
	public void persist(List<BufferedWriter> bws);

}
