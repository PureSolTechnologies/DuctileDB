package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;

public class DuctileVariableFeatures implements VariableFeatures {

    @Override
    public boolean supportsVariables() {
	return true;
    }

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
	return false;
    }

    @Override
    public boolean supportsMixedListValues() {
	return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
	return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
	return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
	return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
	return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
	return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
	return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
	return false;
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
	return false;
    }

}
