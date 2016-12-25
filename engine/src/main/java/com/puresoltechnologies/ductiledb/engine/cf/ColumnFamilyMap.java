package com.puresoltechnologies.ductiledb.engine.cf;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.puresoltechnologies.ductiledb.logstore.Key;

public class ColumnFamilyMap implements NavigableMap<Key, ColumnMap> {

    private static final long serialVersionUID = -7586796985932403392L;

    private final TreeMap<Key, ColumnMap> map = new TreeMap<>();

    @Override
    public String toString() {
	StringBuilder buffer = new StringBuilder();
	for (Entry<Key, ColumnMap> columnFamily : entrySet()) {
	    if (buffer.length() > 0) {
		buffer.append('\n');
	    }
	    buffer.append("family: ");
	    buffer.append(columnFamily.getKey());
	    buffer.append("\n  ");
	    buffer.append(columnFamily.getValue().toString().replaceAll("\\n", "\n    "));
	}
	return buffer.toString();
    }

    @Override
    public boolean isEmpty() {
	return map.isEmpty();
    }

    @Override
    public int size() {
	return map.size();
    }

    @Override
    public boolean containsKey(Object key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
	if ((value == null) || (!ColumnValue.class.isAssignableFrom(value.getClass()))) {
	    throw new IllegalArgumentException("Value ist not of type '" + ColumnValue.class.getName() + "'.");
	}
	return map.containsValue(value);
    }

    @Override
    public ColumnMap get(Object key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.get(key);
    }

    @Override
    public Comparator<? super Key> comparator() {
	return map.comparator();
    }

    @Override
    public Key firstKey() {
	return map.firstKey();
    }

    @Override
    public Key lastKey() {
	return map.lastKey();
    }

    @Override
    public void putAll(Map<? extends Key, ? extends ColumnMap> map) {
	this.map.putAll(map);
    }

    @Override
    public boolean equals(Object o) {
	return map.equals(o);
    }

    @Override
    public int hashCode() {
	return map.hashCode();
    }

    @Override
    public ColumnMap put(Key key, ColumnMap value) {
	return map.put(key, value);
    }

    @Override
    public ColumnMap remove(Object key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.remove(key);
    }

    @Override
    public void clear() {
	map.clear();
    }

    @Override
    public Object clone() {
	return map.clone();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> firstEntry() {
	return map.firstEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> lastEntry() {
	return map.lastEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> pollFirstEntry() {
	return map.pollFirstEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> pollLastEntry() {
	return map.pollLastEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> lowerEntry(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.lowerEntry(key);
    }

    @Override
    public Key lowerKey(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.lowerKey(key);
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> floorEntry(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.floorEntry(key);
    }

    @Override
    public Key floorKey(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.floorKey(key);
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> ceilingEntry(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.ceilingEntry(key);
    }

    @Override
    public Key ceilingKey(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.ceilingKey(key);
    }

    @Override
    public java.util.Map.Entry<Key, ColumnMap> higherEntry(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.higherEntry(key);
    }

    @Override
    public Key higherKey(Key key) {
	if ((key == null) || (!Key.class.isAssignableFrom(key.getClass()))) {
	    throw new IllegalArgumentException("Key ist not of type '" + Key.class.getName() + "'.");
	}
	return map.higherKey(key);
    }

    @Override
    public Set<Key> keySet() {
	return map.keySet();
    }

    @Override
    public NavigableSet<Key> navigableKeySet() {
	return map.navigableKeySet();
    }

    @Override
    public NavigableSet<Key> descendingKeySet() {
	return map.descendingKeySet();
    }

    @Override
    public Collection<ColumnMap> values() {
	return map.values();
    }

    @Override
    public Set<java.util.Map.Entry<Key, ColumnMap>> entrySet() {
	return map.entrySet();
    }

    @Override
    public NavigableMap<Key, ColumnMap> descendingMap() {
	return map.descendingMap();
    }

    @Override
    public NavigableMap<Key, ColumnMap> subMap(Key fromKey, boolean fromInclusive, Key toKey, boolean toInclusive) {
	return map.subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public NavigableMap<Key, ColumnMap> headMap(Key toKey, boolean inclusive) {
	return map.headMap(toKey, inclusive);
    }

    @Override
    public NavigableMap<Key, ColumnMap> tailMap(Key fromKey, boolean inclusive) {
	return map.tailMap(fromKey, inclusive);
    }

    @Override
    public SortedMap<Key, ColumnMap> subMap(Key fromKey, Key toKey) {
	return map.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<Key, ColumnMap> headMap(Key toKey) {
	return map.headMap(toKey);
    }

    @Override
    public SortedMap<Key, ColumnMap> tailMap(Key fromKey) {
	return map.tailMap(fromKey);
    }

    @Override
    public boolean replace(Key key, ColumnMap oldValue, ColumnMap newValue) {
	return map.replace(key, oldValue, newValue);
    }

    @Override
    public ColumnMap replace(Key key, ColumnMap value) {
	return map.replace(key, value);
    }

    @Override
    public void forEach(BiConsumer<? super Key, ? super ColumnMap> action) {
	map.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super Key, ? super ColumnMap, ? extends ColumnMap> function) {
	map.replaceAll(function);
    }
}
