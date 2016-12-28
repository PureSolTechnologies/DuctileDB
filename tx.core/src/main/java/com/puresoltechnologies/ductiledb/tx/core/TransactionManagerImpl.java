package com.puresoltechnologies.ductiledb.tx.core;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

public class TransactionManagerImpl implements TransactionManager {

    private ThreadLocal<TransactionImpl> localTransaction = ThreadLocal.withInitial(() -> new TransactionImpl(this));

    private int transactionTimeout = TransactionImpl.DEFAULT_TRANSACTION_TIMEOUT;

    @Override
    public void begin() throws NotSupportedException, SystemException {
	TransactionImpl transaction = localTransaction.get();
	transaction.setTimeout(transactionTimeout);
	transaction.restart();
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
	    SecurityException, IllegalStateException, SystemException {
	// TODO Auto-generated method stub
	localTransaction.get().commit();
    }

    @Override
    public int getStatus() throws SystemException {
	return localTransaction.get().getStatus();
    }

    @Override
    public Transaction getTransaction() throws SystemException {
	return localTransaction.get();
    }

    @Override
    public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException, SystemException {
	// TODO Auto-generated method stub

    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
	// TODO Auto-generated method stub
	localTransaction.get().rollback();
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
	localTransaction.get().setRollbackOnly();
    }

    @Override
    public void setTransactionTimeout(int seconds) throws SystemException {
	if (seconds == 0) {
	    seconds = TransactionImpl.DEFAULT_TRANSACTION_TIMEOUT;
	} else if (seconds < 0) {
	    throw new SystemException(
		    "A transaction timeout must be greater than zero or equal to zero to set the default timeout.");
	}
	this.transactionTimeout = seconds;
    }

    @Override
    public Transaction suspend() throws SystemException {
	// TODO Auto-generated method stub
	return null;
    }

}
