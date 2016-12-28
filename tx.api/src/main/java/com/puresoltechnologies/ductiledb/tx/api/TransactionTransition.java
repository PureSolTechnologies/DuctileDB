package com.puresoltechnologies.ductiledb.tx.api;

import com.puresoltechnologies.graph.Pair;
import com.puresoltechnologies.statemodel.StateModel;
import com.puresoltechnologies.statemodel.Transition;

/**
 * This enum contains all available transition of a transaction states
 * implementing also the {@link Transition} interface to be used in a
 * {@link StateModel}.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public enum TransactionTransition implements Transition<TransactionState, TransactionTransition> {
    BLOCK {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.RUNNING, TransactionState.BLOCKED);
	}

	@Override
	public String getName() {
	    return "Block";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.BLOCKED;
	}
    },
    RESUME {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.BLOCKED, TransactionState.RUNNING);
	}

	@Override
	public String getName() {
	    return "Resume";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.RUNNING;
	}
    },
    REJECT_RUNNING {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.RUNNING, TransactionState.ABORTED);
	}

	@Override
	public String getName() {
	    return "Reject";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.ABORTED;
	}
    },
    REJECT_BLOCKED {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.BLOCKED, TransactionState.ABORTED);
	}

	@Override
	public String getName() {
	    return "Reject";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.ABORTED;
	}
    },
    COMMIT {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.RUNNING, TransactionState.COMMITTED);
	}

	@Override
	public String getName() {
	    return "Commit";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.COMMITTED;
	}
    },
    RESTART_COMMITTED {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.COMMITTED, TransactionState.RUNNING);
	}

	@Override
	public String getName() {
	    return "Restart";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.RUNNING;
	}
    },
    RESTART_ABORTED {
	@Override
	public Pair<TransactionState> getVertices() {
	    return new Pair<>(TransactionState.ABORTED, TransactionState.RUNNING);
	}

	@Override
	public String getName() {
	    return "Restart";
	}

	@Override
	public TransactionState getTargetState() {
	    return TransactionState.RUNNING;
	}
    };

}
