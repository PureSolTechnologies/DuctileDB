package com.puresoltechnologies.ductiledb.storage.engine;

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

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This map is used to store columns and their values. The underlying map is a
 * {@link TreeMap} sorting the keys for SSTable files.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnMap implements NavigableMap<byte[], byte[]> {

    private final TreeMap<byte[], byte[]> values = new TreeMap<>(ByteArrayComparator.getInstance());

    public ColumnMap() {
	super();
    }

    @Override
    public String toString() {
	StringBuffer buffer = new StringBuffer();
	for (java.util.Map.Entry<byte[], byte[]> entry : entrySet()) {
	    buffer.append(Bytes.toHumanReadableString(entry.getKey()));
	    buffer.append("=");
	    buffer.append(Bytes.toHumanReadableString(entry.getValue()));
	    buffer.append("\n");
	}
	return buffer.toString();
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((values == null) ? 0 : values.hashCode());
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
	if (size() != other.size()) {
	    return false;
	}
	ByteArrayComparator comparator = ByteArrayComparator.getInstance();
	for (java.util.Map.Entry<byte[], byte[]> entry : entrySet()) {
	    if (comparator.compare(entry.getValue(), other.get(entry.getKey())) != 0) {
		return false;
	    }
	}
	return true;
    }

    @Override
    public int size() {
	return values.size();
    }

    @Override
    public boolean isEmpty() {
	return values.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
	return values.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
	return values.containsValue(value);
    }

    @Override
    public byte[] get(Object key) {
	return values.get(key);
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
	if ((key == null) || (key.length == 0)) {
	    throw new IllegalArgumentException("Column keys must not be null or empty.");
	}
	return values.put(key, value);
    }

    @Override
    public byte[] remove(Object key) {
	return values.remove(key);
    }

    @Override
    public void putAll(Map<? extends byte[], ? extends byte[]> m) {
	values.putAll(m);
    }

    @Override
    public void clear() {
	values.clear();
    }

    @Override
    public Set<byte[]> keySet() {
	return values.keySet();
    }

    @Override
    public Collection<byte[]> values() {
	return values.values();
    }

    @Override
    public Set<java.util.Map.Entry<byte[], byte[]>> entrySet() {
	return values.entrySet();
    }

    @Override
    public Comparator<? super byte[]> comparator() {
	return values.comparator();
    }

    @Override
    public byte[] firstKey() {
	return values.firstKey();
    }

    @Override
    public byte[] lastKey() {
	return values.lastKey();
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> lowerEntry(byte[] key) {
	return values.lowerEntry(key);
    }

    @Override
    public byte[] lowerKey(byte[] key) {
	return values.lowerKey(null);
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> floorEntry(byte[] key) {
	return values.floorEntry(key);
    }

    @Override
    public Object clone() {
	return values.clone();
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> firstEntry() {
	return values.firstEntry();
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> lastEntry() {
	return values.lastEntry();
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> pollFirstEntry() {
	return values.pollFirstEntry();
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> pollLastEntry() {
	return values.pollLastEntry();
    }

    @Override
    public byte[] floorKey(byte[] key) {
	return values.floorKey(key);
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> ceilingEntry(byte[] key) {
	return values.ceilingEntry(key);
    }

    @Override
    public byte[] ceilingKey(byte[] key) {
	return values.ceilingKey(key);
    }

    @Override
    public java.util.Map.Entry<byte[], byte[]> higherEntry(byte[] key) {
	return values.higherEntry(key);
    }

    @Override
    public byte[] higherKey(byte[] key) {
	return values.higherKey(key);
    }

    @Override
    public NavigableSet<byte[]> navigableKeySet() {
	return values.navigableKeySet();
    }

    @Override
    public NavigableSet<byte[]> descendingKeySet() {
	return values.descendingKeySet();
    }

    @Override
    public NavigableMap<byte[], byte[]> descendingMap() {
	return values.descendingMap();
    }

    @Override
    public NavigableMap<byte[], byte[]> subMap(byte[] fromKey, boolean fromInclusive, byte[] toKey,
	    boolean toInclusive) {
	return values.subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public NavigableMap<byte[], byte[]> headMap(byte[] toKey, boolean inclusive) {
	return values.headMap(toKey, inclusive);
    }

    @Override
    public NavigableMap<byte[], byte[]> tailMap(byte[] fromKey, boolean inclusive) {
	return values.tailMap(fromKey, inclusive);
    }

    @Override
    public SortedMap<byte[], byte[]> subMap(byte[] fromKey, byte[] toKey) {
	return values.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<byte[], byte[]> headMap(byte[] toKey) {
	return values.headMap(toKey);
    }

    @Override
    public SortedMap<byte[], byte[]> tailMap(byte[] fromKey) {
	return values.tailMap(fromKey);
    }

    @Override
    public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
	return values.replace(key, oldValue, newValue);
    }

    @Override
    public byte[] replace(byte[] key, byte[] value) {
	return values.replace(key, value);
    }

    @Override
    public void forEach(BiConsumer<? super byte[], ? super byte[]> action) {
	values.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super byte[], ? super byte[], ? extends byte[]> function) {
	values.replaceAll(function);
    }
}
