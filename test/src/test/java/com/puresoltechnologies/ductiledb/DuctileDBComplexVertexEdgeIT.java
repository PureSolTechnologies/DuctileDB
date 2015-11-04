package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.fail;

import org.junit.Test;

public class DuctileDBComplexVertexEdgeIT extends AbstractDuctileDBGraphTest {

    /**
     * Checks whether a vertex removal leads also to the complete removal of all
     * edges and their indizes.
     */
    @Test
    public void testRemoveVertexWithEdges() {
	fail();
    }

    /**
     * Tests lazy loading of vertices after edge loading to not have a complete
     * eager graph loading.
     */
    @Test
    public void testLazyVertexLoadingInEdges() {
	fail();
    }
}
