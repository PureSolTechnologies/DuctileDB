package com.puresoltechnologies.ductiledb.xo.impl;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;

public class DuctileStoreTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNullConfigFilePath() throws IOException {
	assertNotNull(new DuctileStore(null));
    }

}
