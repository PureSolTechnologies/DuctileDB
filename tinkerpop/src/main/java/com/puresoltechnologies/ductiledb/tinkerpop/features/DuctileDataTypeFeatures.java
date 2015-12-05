package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures;

public class DuctileDataTypeFeatures implements DataTypeFeatures {

    @Override
    public boolean supportsBooleanValues() {
	return true;
    }

    @Override
    public boolean supportsByteValues() {
	return true;
    }

    @Override
    public boolean supportsDoubleValues() {
	return true;
    }

    @Override
    public boolean supportsFloatValues() {
	return true;
    }

    @Override
    public boolean supportsIntegerValues() {
	return true;
    }

    @Override
    public boolean supportsLongValues() {
	return true;
    }

    @Override
    public boolean supportsMapValues() {
	return true;
    }

    @Override
    public boolean supportsMixedListValues() {
	return true;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
	return true;
    }

    @Override
    public boolean supportsByteArrayValues() {
	return true;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
	return true;
    }

    @Override
    public boolean supportsFloatArrayValues() {
	return true;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
	return true;
    }

    @Override
    public boolean supportsStringArrayValues() {
	return true;
    }

    @Override
    public boolean supportsLongArrayValues() {
	return true;
    }

    @Override
    public boolean supportsSerializableValues() {
	return true;
    }

    @Override
    public boolean supportsStringValues() {
	return true;
    }

    @Override
    public boolean supportsUniformListValues() {
	return true;
    }

}
