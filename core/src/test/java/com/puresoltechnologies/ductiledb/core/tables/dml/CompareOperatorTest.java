package com.puresoltechnologies.ductiledb.core.tables.dml;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CompareOperatorTest<T extends Comparable<T>> {

    private static class TestParameter<T> {

	private final T smaller;
	private final T larger;

	public TestParameter(T smaller, T larger) {
	    this.smaller = smaller;
	    this.larger = larger;
	}

	public Object getSmaller() {
	    return smaller;
	}

	public Object getLarger() {
	    return larger;
	}
    }

    @Parameters(name = "{0}({1}) {2} {3}")
    public static Collection<Object[]> getOperators() {
	CompareOperator[] operators = CompareOperator.values();
	List<Object[]> parameters = new ArrayList<>();
	for (int i = 0; i < operators.length; ++i) {
	    CompareOperator operator = operators[i];
	    boolean matchesOriginalOrder;
	    boolean matchesReverseOrder;
	    boolean matchesSame;
	    switch (operator) {
	    case EQUALS:
		matchesOriginalOrder = false;
		matchesReverseOrder = false;
		matchesSame = true;
		break;
	    case LESS_THEN:
		matchesOriginalOrder = true;
		matchesReverseOrder = false;
		matchesSame = false;
		break;
	    case LESS_OR_EQUAL:
		matchesOriginalOrder = true;
		matchesReverseOrder = false;
		matchesSame = true;
		break;
	    case GREATER_THAN:
		matchesOriginalOrder = false;
		matchesReverseOrder = true;
		matchesSame = false;
		break;
	    case GREATER_OR_EQUAL:
		matchesOriginalOrder = false;
		matchesReverseOrder = true;
		matchesSame = true;
		break;
	    default:
		fail("Not defined for operator '" + operator.name() + "'.");
		return null;
	    }
	    parameters.add(new Object[] { operator, Byte.class, (byte) -1, (byte) 1, matchesOriginalOrder,
		    matchesReverseOrder, matchesSame });
	    parameters.add(new Object[] { operator, Short.class, (short) -1, (short) 1, matchesOriginalOrder,
		    matchesReverseOrder, matchesSame });
	    parameters.add(new Object[] { operator, Integer.class, -1, 1, matchesOriginalOrder, matchesReverseOrder,
		    matchesSame });
	    parameters.add(new Object[] { operator, Long.class, (long) -1, (long) 1, matchesOriginalOrder,
		    matchesReverseOrder, matchesSame });
	    parameters.add(new Object[] { operator, Float.class, (float) -1.0, (float) 1.1, matchesOriginalOrder,
		    matchesReverseOrder, matchesSame });
	    parameters.add(new Object[] { operator, Double.class, -1.0, 1.0, matchesOriginalOrder, matchesReverseOrder,
		    matchesSame });
	    parameters.add(new Object[] { operator, String.class, "a", "z", matchesOriginalOrder, matchesReverseOrder,
		    matchesSame });

	}
	return parameters;
    }

    private final CompareOperator operator;
    private final T smaller;
    private final T larger;
    private final boolean matchesOriginalOrder;
    private final boolean matchesReverseOrder;
    private final boolean matchesSame;

    public CompareOperatorTest(CompareOperator operator, Class<T> clazz, T smaller, T larger,
	    boolean matchesOriginalOrder, boolean matchesReverseOrder, boolean matchesSame) {
	super();
	this.operator = operator;
	this.smaller = smaller;
	this.larger = larger;
	this.matchesOriginalOrder = matchesOriginalOrder;
	this.matchesReverseOrder = matchesReverseOrder;
	this.matchesSame = matchesSame;
    }

    @Test
    public void testSmallerOperationLarger() {
	Assert.assertEquals(matchesOriginalOrder, operator.matches(smaller, larger));
    }

    @Test
    public void testLargerOperationSmaller() {
	Assert.assertEquals(matchesReverseOrder, operator.matches(larger, smaller));
    }

    @Test
    public void testValueOperationSame() {
	Assert.assertEquals(matchesSame, operator.matches(smaller, smaller));
	Assert.assertEquals(matchesSame, operator.matches(larger, larger));
    }

}
