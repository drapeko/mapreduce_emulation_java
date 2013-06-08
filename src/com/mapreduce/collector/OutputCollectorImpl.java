package com.mapreduce.collector;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mapreduce.logic.Combiner;
import com.mapreduce.partitioner.Partitioner;
import com.mapreduce.util.Const;

public class OutputCollectorImpl<K, V> implements OutputCollector<K, V> {
	
	private List<Map.Entry<K, V>> localStorage = Collections.synchronizedList(new ArrayList<Map.Entry<K, V>>());
	private Partitioner<K> partitioner;
	private Combiner<K, V> combiner;
	
	public OutputCollectorImpl(Partitioner<K> partitioner, Combiner<K, V> combiner) {
		this.partitioner = partitioner;
		this.combiner = combiner;
	}
	
	@Override
	public void collect(K key, V value) {
		Map.Entry<K, V> entry = new AbstractMap.SimpleEntry<K, V>(key, value);
		localStorage.add(entry);
	}

	private List<Map.Entry<K, V>> combine() {
		if (combiner != null) {
			List<Map.Entry<K, V>> combinedStorage = new ArrayList<Map.Entry<K, V>>();
			
			Map<K, List<V>> map = new HashMap<K, List<V>>();

			for (Map.Entry<K, V> entry : localStorage) {
				if (!map.containsKey(entry.getKey())) {
					map.put(entry.getKey(), new ArrayList<V>());
				}
				map.get(entry.getKey()).add(entry.getValue());				
			}
			
			
			for (Map.Entry<K, List<V>> mapEntry : map.entrySet()) {
				Map.Entry<K, V> combined = combiner.combine(mapEntry.getKey(), mapEntry.getValue());
				combinedStorage.add(combined);
			}
			
			return combinedStorage;
		}
		return localStorage;
	}

	@Override
	public void persist(List<BufferedWriter> bws) {
		List<Map.Entry<K, V>> storage = combine();
		
		try {
			for(Map.Entry<K, V> entry : storage) {
				int idx = partitioner.partition(entry.getKey());
				BufferedWriter bw = bws.get(idx);
				bw.append(entry.getKey().toString() + Const.KEY_VALUE_SEPARATOR + entry.getValue().toString() + Const.LINE_SEPARATOR);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
