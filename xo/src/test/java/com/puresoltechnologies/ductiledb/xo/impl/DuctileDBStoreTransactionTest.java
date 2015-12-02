package com.puresoltechnologies.ductiledb.xo.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.buschmais.xo.api.XOException;
import com.puresoltechnologies.ductiledb.core.core.core.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStoreTransaction;

/**
 * This unit test checks the logic for active state and initialization.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBStoreTransactionTest {

    private final static DuctileDBGraph titanGraphMock = mock(DuctileDBGraph.class);

    @Before
    public void initialize() {
	DuctileDBTransaction transactionMock = mock(DuctileDBTransaction.class);
	when(transactionMock.isOpen()).thenReturn(true);
	when(transactionMock.isClosed()).thenReturn(false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTitanGraph() {
	new DuctileDBStoreTransaction(null);
    }

    @Test
    public void testBeginCommitActive() {
	DuctileDBStoreTransaction transaction = new DuctileDBStoreTransaction(titanGraphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.commit();
	assertFalse(transaction.isActive());
    }

    @Test
    public void testBeginRollbackActive() {
	DuctileDBStoreTransaction transaction = new DuctileDBStoreTransaction(titanGraphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.rollback();
	assertFalse(transaction.isActive());
    }

    @Test(expected = XOException.class)
    public void testBeginDoubleCommit() {
	DuctileDBStoreTransaction transaction = new DuctileDBStoreTransaction(titanGraphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.commit();
	assertFalse(transaction.isActive());
	transaction.commit();
    }

    @Test(expected = XOException.class)
    public void testBeginDoubleRollback() {
	DuctileDBStoreTransaction transaction = new DuctileDBStoreTransaction(titanGraphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.rollback();
	assertFalse(transaction.isActive());
	transaction.rollback();
    }

    @Test(expected = XOException.class)
    public void testDoubleBegin() {
	DuctileDBStoreTransaction transaction = new DuctileDBStoreTransaction(titanGraphMock);
	assertFalse(transaction.isActive());
	transaction.begin();
	assertTrue(transaction.isActive());
	transaction.begin();
    }

}
