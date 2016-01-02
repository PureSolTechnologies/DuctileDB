package com.puresoltechnologies.ductiledb.core.utils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;

import com.esotericsoftware.kryo.Kryo;

/**
 * This class is a facade for the serialization of value.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Serializer {

    private static final Kryo kryo = new Kryo();

    /**
     * Private constructor to avoid instantiation.
     */
    private Serializer() {
    }

    public static void serialize(Serializable object, OutputStream outputStream) {
	SerializationUtils.serialize(object, outputStream);
    }

    public static byte[] serialize(Serializable object) {
	return SerializationUtils.serialize(object);
    }

    public static <T> T deserialize(InputStream inputStream) {
	Object object = SerializationUtils.deserialize(inputStream);
	@SuppressWarnings("unchecked")
	T t = (T) object;
	return t;
    }

    public static <T> T deserialize(byte[] bytes) {
	Object object = SerializationUtils.deserialize(bytes);
	@SuppressWarnings("unchecked")
	T t = (T) object;
	return t;
    }
}
