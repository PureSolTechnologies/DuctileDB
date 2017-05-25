package com.puresoltechnologies.ductiledb.columnfamily;

import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class CompoundKey extends Key {

    public static byte[][] decode(byte[] key) {
	int num = key[0] & 0xFF;
	byte[][] keyParts = new byte[num][];
	int pos = 1;
	for (int i = 0; i < num; ++i) {
	    int length = Bytes.toInt(key, pos);
	    pos += 4;
	    byte[] keyPart = new byte[length];
	    System.arraycopy(key, pos, keyPart, 0, length);
	    pos += length;
	    keyParts[i] = keyPart;
	}
	return keyParts;
    }

    public static byte[] encode(byte[]... keyParts) {
	if ((keyParts.length == 0) || (keyParts.length > 255)) {
	    throw new IllegalArgumentException(
		    "The number of key pars needs to be greater than zero and less than or equal 255.");
	}
	int keySize = 1; // 1 byte for number parts
	for (byte[] keyPart : keyParts) {
	    keySize += 4 + keyPart.length;
	}
	byte[] key = new byte[keySize];
	key[0] = (byte) keyParts.length;
	int pos = 1;
	for (byte[] keyPart : keyParts) {
	    System.arraycopy(Bytes.toBytes(keyPart.length), 0, key, pos, 4);
	    pos += 4;
	    System.arraycopy(keyPart, 0, key, pos, keyPart.length);
	    pos += keyPart.length;
	}
	return key;
    }

    public static CompoundKey create(byte[]... keyParts) {
	return new CompoundKey(false, keyParts);
    }

    public static CompoundKey of(byte[] compoundKey) {
	return new CompoundKey(true, compoundKey);
    }

    public static CompoundKey create(Key... keyParts) {
	byte[][] parts = new byte[keyParts.length][];
	for (int i = 0; i < keyParts.length; ++i) {
	    Key keyPart = keyParts[i];
	    parts[i] = keyPart.getBytes();
	}
	return create(parts);
    }

    public static CompoundKey of(Key compoundKey) {
	return of(compoundKey.getBytes());
    }

    List<byte[]> parts = new ArrayList<>();

    private CompoundKey(boolean encoded, byte[]... keyParts) {
	super(encoded ? keyParts[0] : encode(keyParts));
	if (encoded) {
	    keyParts = decode(keyParts[0]);
	}
	for (byte[] keyPart : keyParts) {
	    parts.add(keyPart);
	}
    }

    public int getPartNum() {
	return getBytes()[0] & 0xFF;
    }

    public byte[] getPart(int i) {
	return parts.get(i);
    }

}
