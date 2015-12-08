package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

public class DuctileMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    public final Map<K, Queue<V>> reduceMap;
    public final Queue<KeyValue<K, V>> mapQueue;
    private final boolean doReduce;

    public DuctileMapEmitter(boolean doReduce) {
	this.doReduce = doReduce;
	if (this.doReduce) {
	    reduceMap = new ConcurrentHashMap<>();
	    mapQueue = null;
	} else {
	    reduceMap = null;
	    mapQueue = new ConcurrentLinkedQueue<>();
	}
    }

    @Override
    public void emit(K key, V value) {
	if (doReduce)
	    reduceMap.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(value);
	else
	    mapQueue.add(new KeyValue<>(key, value));
    }

    protected void complete(MapReduce<K, V, ?, ?, ?> mapReduce) {
	if (!doReduce && mapReduce.getMapKeySort().isPresent()) {
	    Comparator<K> comparator = mapReduce.getMapKeySort().get();
	    List<KeyValue<K, V>> list = new ArrayList<>(mapQueue);
	    Collections.sort(list, Comparator.comparing(KeyValue::getKey, comparator));
	    mapQueue.clear();
	    mapQueue.addAll(list);
	} else if (mapReduce.getMapKeySort().isPresent()) {
	    Comparator<K> comparator = mapReduce.getMapKeySort().get();
	    List<Map.Entry<K, Queue<V>>> list = new ArrayList<>();
	    list.addAll(reduceMap.entrySet());
	    Collections.sort(list, Comparator.comparing(Map.Entry::getKey, comparator));
	    reduceMap.clear();
	    list.forEach(entry -> reduceMap.put(entry.getKey(), entry.getValue()));
	}
    }

}
