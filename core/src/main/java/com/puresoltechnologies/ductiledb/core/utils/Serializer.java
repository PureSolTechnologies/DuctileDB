package com.puresoltechnologies.ductiledb.core.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This class is a facade for the serialization of value.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Serializer {

    private static final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> new Kryo());

    /**
     * Private constructor to avoid instantiation.
     */
    private Serializer() {
    }

    public static <T extends Serializable> void serialize(T object, OutputStream outputStream, Class<T> clazz) {
	Kryo kryo = kryos.get();
	try (Output output = new Output(outputStream)) {
	    kryo.writeObject(output, object);
	    output.flush();
	}
    }

    public static <T extends Serializable> byte[] serialize(T object, Class<T> clazz) throws IOException {
	Kryo kryo = kryos.get();
	try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
	    try (Output output = new Output(outputStream)) {
		kryo.writeClassAndObject(output, object);
		output.flush();
		return outputStream.toByteArray();
	    }
	}
    }

    public static <T extends Serializable> byte[] serializePropertyValue(T object) throws IOException {
	Kryo kryo = kryos.get();
	try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
	    try (Output output = new Output(outputStream)) {
		kryo.writeClassAndObject(output, object);
		output.flush();
		return outputStream.toByteArray();
	    }
	}
    }

    public static <T extends Serializable> T deserialize(InputStream inputStream, Class<T> clazz) {
	Kryo kryo = kryos.get();
	try (Input input = new Input(inputStream)) {
	    Object object = kryo.readObject(input, clazz);
	    @SuppressWarnings("unchecked")
	    T t = (T) object;
	    return t;
	}
    }

    public static <T extends Serializable> T deserialize(byte[] bytes, Class<T> clazz) {
	Kryo kryo = kryos.get();
	try (Input input = new Input(bytes)) {
	    Object object = kryo.readObject(input, clazz);
	    @SuppressWarnings("unchecked")
	    T t = (T) object;
	    return t;
	}
    }

    public static <T extends Serializable> T deserializePropertyValue(byte[] bytes) {
	Kryo kryo = kryos.get();
	if ((bytes == null) || (bytes.length == 0)) {
	    return null;
	}
	try (Input input = new Input(bytes)) {
	    Object object = kryo.readClassAndObject(input);
	    @SuppressWarnings("unchecked")
	    T t = (T) object;
	    return t;
	}
    }
}
