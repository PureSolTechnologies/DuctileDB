package com.puresoltechnologies.ductiledb.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

interface DuctileWrapper extends Wrapper {

    @Override
    default public <T> T unwrap(Class<T> iface) throws SQLException {
	if (iface.isAssignableFrom(this.getClass())) {
	    @SuppressWarnings("unchecked")
	    T t = (T) this;
	    return t;
	}
	return null;
    }

    @Override
    default public boolean isWrapperFor(Class<?> iface) throws SQLException {
	return false;
    }

    void checkClosed() throws SQLException;
}
