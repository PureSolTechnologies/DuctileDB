package com.puresoltechnologies.ductiledb.tx.api;

import java.util.HashSet;
import java.util.Set;

import javax.transaction.Status;

import com.puresoltechnologies.statemodel.State;
import com.puresoltechnologies.statemodel.StateModel;

/**
 * This enum contains all available states of a transaction implementing also
 * the {@link State} interface to be used in a {@link StateModel}.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public enum TransactionState implements State<TransactionState, TransactionTransition> {

    RUNNING {

	@Override
	public String getName() {
	    return "Running";
	}

	@Override
	public Set<TransactionTransition> getTransitions() {
	    Set<TransactionTransition> transitions = new HashSet<>();
	    transitions.add(TransactionTransition.BLOCK);
	    transitions.add(TransactionTransition.REJECT_RUNNING);
	    transitions.add(TransactionTransition.COMMIT);
	    return transitions;
	}

	@Override
	public int getJTAStatus() {
	    return Status.STATUS_ACTIVE;
	}

    },
    BLOCKED {

	@Override
	public String getName() {
	    return "Blocked";
	}

	@Override
	public Set<TransactionTransition> getTransitions() {
	    Set<TransactionTransition> transitions = new HashSet<>();
	    transitions.add(TransactionTransition.RESUME);
	    transitions.add(TransactionTransition.REJECT_BLOCKED);
	    return transitions;
	}

	@Override
	public int getJTAStatus() {
	    return Status.STATUS_ACTIVE;
	}

    },
    COMMITTED {

	@Override
	public String getName() {
	    return "Committed";
	}

	@Override
	public Set<TransactionTransition> getTransitions() {
	    Set<TransactionTransition> transitions = new HashSet<>();
	    transitions.add(TransactionTransition.RESTART_COMMITTED);
	    return transitions;
	}

	@Override
	public int getJTAStatus() {
	    return Status.STATUS_COMMITTED;
	}

    },
    ABORTED {

	@Override
	public String getName() {
	    return "Aborted";
	}

	@Override
	public Set<TransactionTransition> getTransitions() {
	    Set<TransactionTransition> transitions = new HashSet<>();
	    transitions.add(TransactionTransition.RESTART_ABORTED);
	    return transitions;
	}

	@Override
	public int getJTAStatus() {
	    return Status.STATUS_ROLLEDBACK;
	}

    };

    public abstract int getJTAStatus();
}
