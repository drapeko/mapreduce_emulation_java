package com.mapreduce.logic;

import java.util.List;
import java.util.Map;

public interface Combiner<K, V> {

	Map.Entry<K, V> combine(K key, List<V> values);
}
