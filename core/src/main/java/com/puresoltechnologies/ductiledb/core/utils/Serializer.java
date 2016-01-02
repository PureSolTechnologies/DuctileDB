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

    public static void serialize(Serializable object, OutputStream outputStream) {
	Kryo kryo = kryos.get();
	try (Output output = new Output(outputStream)) {
	    kryo.writeClassAndObject(output, object);
	    output.flush();
	}
    }

    public static byte[] serialize(Serializable object) throws IOException {
	Kryo kryo = kryos.get();
	try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
	    try (Output output = new Output(outputStream)) {
		kryo.writeClassAndObject(output, object);
		output.flush();
		return outputStream.toByteArray();
	    }
	}
    }

    public static <T> T deserialize(InputStream inputStream) {
	Kryo kryo = kryos.get();
	try (Input input = new Input(inputStream)) {
	    Object object = kryo.readClassAndObject(input);
	    @SuppressWarnings("unchecked")
	    T t = (T) object;
	    return t;
	}
    }

    public static <T> T deserialize(byte[] bytes) {
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
