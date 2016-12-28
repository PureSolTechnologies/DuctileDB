package com.puresoltechnologies.ductiledb.tx.core;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import com.puresoltechnologies.ductiledb.tx.api.TransactionState;

public class TransactionImpl implements Transaction {

    public final static int DEFAULT_TRANSACTION_TIMEOUT = 10; // seconds

    private final TransactionStateModel state = new TransactionStateModel();

    private int timeout = DEFAULT_TRANSACTION_TIMEOUT;
    private Synchronization synchronization = null;
    private final TransactionManagerImpl transactionManager;

    public TransactionImpl(TransactionManagerImpl transactionManager) {
	this.transactionManager = transactionManager;
    }

    public void restart() {
	// TODO Auto-generated method stub
	state.goTo(TransactionState.RUNNING);
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
	    SecurityException, IllegalStateException, SystemException {
	// TODO Auto-generated method stub
	state.goTo(TransactionState.COMMITTED);
    }

    @Override
    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException, SystemException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public int getStatus() throws SystemException {
	return state.getState().getJTAStatus();
    }

    @Override
    public void registerSynchronization(Synchronization sync)
	    throws RollbackException, IllegalStateException, SystemException {
	// TODO Auto-generated method stub

    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
	// TODO Auto-generated method stub
	state.goTo(TransactionState.ABORTED);
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
	// TODO Auto-generated method stub

    }

    public void setTimeout(int seconds) {
	this.timeout = seconds;
    }

}
