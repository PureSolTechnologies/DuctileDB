package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;

public class DuctileReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

    protected Queue<KeyValue<OK, OV>> reduceQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void emit(OK key, OV value) {
	reduceQueue.add(new KeyValue<>(key, value));
    }

    protected void complete(MapReduce<?, ?, OK, OV, ?> mapReduce) {
	if (mapReduce.getReduceKeySort().isPresent()) {
	    Comparator<OK> comparator = mapReduce.getReduceKeySort().get();
	    List<KeyValue<OK, OV>> list = new ArrayList<>(reduceQueue);
	    Collections.sort(list, Comparator.comparing(KeyValue::getKey, comparator));
	    reduceQueue.clear();
	    reduceQueue.addAll(list);
	}
    }
}
