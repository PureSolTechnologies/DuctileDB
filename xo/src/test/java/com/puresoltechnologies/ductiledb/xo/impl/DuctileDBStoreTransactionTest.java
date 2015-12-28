package com.puresoltechnologies.ductiledb.xo.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.buschmais.xo.api.XOException;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;

/**
 * This unit test checks the logic for active state and initialization.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBStoreTransactionTest {

    private final static DuctileGraph graphMock = mock(DuctileGraph.class);

    @Before
    public void initialize() {
	DuctileDBTransaction transactionMock = mock(DuctileDBTransaction.class);
	when(transactionMock.isOpen()).thenReturn(true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTitanGraph() {
	new DuctileStoreTransaction(null);
    }

    @Test
    public void testBeginCommitActive() {
	DuctileStoreTransaction transaction = new DuctileStoreTransaction(graphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.commit();
	assertFalse(transaction.isActive());
    }

    @Test
    public void testBeginRollbackActive() {
	DuctileStoreTransaction transaction = new DuctileStoreTransaction(graphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.rollback();
	assertFalse(transaction.isActive());
    }

    @Test(expected = XOException.class)
    public void testBeginDoubleCommit() {
	DuctileStoreTransaction transaction = new DuctileStoreTransaction(graphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.commit();
	assertFalse(transaction.isActive());
	transaction.commit();
    }

    @Test(expected = XOException.class)
    public void testBeginDoubleRollback() {
	DuctileStoreTransaction transaction = new DuctileStoreTransaction(graphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.rollback();
	assertFalse(transaction.isActive());
	transaction.rollback();
    }

    @Test(expected = XOException.class)
    public void testDoubleBegin() {
	DuctileStoreTransaction transaction = new DuctileStoreTransaction(graphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.begin();
    }

}
