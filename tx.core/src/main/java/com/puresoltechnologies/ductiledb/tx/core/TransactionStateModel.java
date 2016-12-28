package com.puresoltechnologies.ductiledb.tx.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.puresoltechnologies.ductiledb.tx.api.TransactionState;
import com.puresoltechnologies.ductiledb.tx.api.TransactionTransition;
import com.puresoltechnologies.statemodel.AbstractStateModel;
import com.puresoltechnologies.statemodel.StateModel;

/**
 * This model is used to define and watch the correct state of a transaction.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TransactionStateModel extends AbstractStateModel<TransactionState, TransactionTransition>
	implements StateModel<TransactionState, TransactionTransition> {

    @Override
    public Set<TransactionState> getVertices() {
	TransactionState[] values = TransactionState.values();
	return new HashSet<>(Arrays.asList(values));
    }

    @Override
    public TransactionState getStartState() {
	return TransactionState.RUNNING;
    }

    @Override
    public Set<TransactionState> getEndStates() {
	return new HashSet<>();
    }

}
