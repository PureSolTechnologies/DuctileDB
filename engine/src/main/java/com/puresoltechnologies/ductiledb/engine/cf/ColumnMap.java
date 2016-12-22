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

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;

/**
 * This map is used to store columns and their values. The underlying map is a
 * {@link TreeMap} sorting the keys for SSTable files.
 * 
 * @author Rick-Rainer Ludwig
 */
public final class ColumnMap implements NavigableMap<Key, ColumnValue> {

    private final TreeMap<Key, ColumnValue> map = new TreeMap<>();

    public ColumnMap() {
	super();
    }

    public ColumnKeySet getColumnKeySet() {
	return new ColumnKeySet(keySet());
    }

    @Override
    public String toString() {
	StringBuilder buffer = new StringBuilder();
	for (java.util.Map.Entry<? extends Key, ? extends ColumnValue> entry : entrySet()) {
	    buffer.append(entry.getKey().toString());
	    buffer.append("=");
	    ColumnValue value = entry.getValue();
	    buffer.append(Bytes.toHumanReadableString(value.getBytes()));
	    if (value.wasDeleted()) {
		buffer.append(" (deleted)");
	    }
	    buffer.append("\n");
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
    public ColumnValue get(Object key) {
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
    public void putAll(Map<? extends Key, ? extends ColumnValue> map) {
	this.map.putAll(map);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((map == null) ? 0 : map.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	ColumnMap other = (ColumnMap) obj;
	if (map == null) {
	    if (other.map != null)
		return false;
	} else if (!map.equals(other.map))
	    return false;
	return true;
    }

    @Override
    public ColumnValue put(Key key, ColumnValue value) {
	return map.put(key, value);
    }

    @Override
    public ColumnValue remove(Object key) {
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
    public java.util.Map.Entry<Key, ColumnValue> firstEntry() {
	return map.firstEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnValue> lastEntry() {
	return map.lastEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnValue> pollFirstEntry() {
	return map.pollFirstEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnValue> pollLastEntry() {
	return map.pollLastEntry();
    }

    @Override
    public java.util.Map.Entry<Key, ColumnValue> lowerEntry(Key key) {
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
    public java.util.Map.Entry<Key, ColumnValue> floorEntry(Key key) {
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
    public java.util.Map.Entry<Key, ColumnValue> ceilingEntry(Key key) {
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
    public java.util.Map.Entry<Key, ColumnValue> higherEntry(Key key) {
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
    public Collection<ColumnValue> values() {
	return map.values();
    }

    @Override
    public Set<java.util.Map.Entry<Key, ColumnValue>> entrySet() {
	return map.entrySet();
    }

    @Override
    public NavigableMap<Key, ColumnValue> descendingMap() {
	return map.descendingMap();
    }

    @Override
    public NavigableMap<Key, ColumnValue> subMap(Key fromKey, boolean fromInclusive, Key toKey, boolean toInclusive) {
	return map.subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public NavigableMap<Key, ColumnValue> headMap(Key toKey, boolean inclusive) {
	return map.headMap(toKey, inclusive);
    }

    @Override
    public NavigableMap<Key, ColumnValue> tailMap(Key fromKey, boolean inclusive) {
	return map.tailMap(fromKey, inclusive);
    }

    @Override
    public SortedMap<Key, ColumnValue> subMap(Key fromKey, Key toKey) {
	return map.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<Key, ColumnValue> headMap(Key toKey) {
	return map.headMap(toKey);
    }

    @Override
    public SortedMap<Key, ColumnValue> tailMap(Key fromKey) {
	return map.tailMap(fromKey);
    }

    @Override
    public boolean replace(Key key, ColumnValue oldValue, ColumnValue newValue) {
	return map.replace(key, oldValue, newValue);
    }

    @Override
    public ColumnValue replace(Key key, ColumnValue value) {
	return map.replace(key, value);
    }

    @Override
    public void forEach(BiConsumer<? super Key, ? super ColumnValue> action) {
	map.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super Key, ? super ColumnValue, ? extends ColumnValue> function) {
	map.replaceAll(function);
    }

}
