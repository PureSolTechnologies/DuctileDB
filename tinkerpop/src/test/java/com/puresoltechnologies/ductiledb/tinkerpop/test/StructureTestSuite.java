package com.puresoltechnologies.ductiledb.tinkerpop.test;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.TransactionTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * <p>
 * The {@code StructureStandardSuite} is a JUnit test runner that executes the
 * Gremlin Test Suite over a {@link Graph} implementation. This specialized test
 * suite and runner is for use by vendors to test their {@link Graph}
 * implementations. The {@code StructureStandardSuite} ensures consistency and
 * validity of the implementations that they test. Successful execution of this
 * test suite is critical to proper operations of a vendor implementation.
 * </p>
 * <p>
 * To use the {@code StructureStandardSuite} define a class in a test module.
 * Simple naming would expect the name of the implementation followed by
 * "StructureStandardSuite". This class should be annotated as follows (note
 * that the "Suite" implements {@link GraphProvider} as a convenience only. It
 * could be implemented in a separate class file): <code>
 * &#064;RunWith(StructureStandardSuite.class)
 * &#064;StructureStandardSuite.GraphProviderClass(TinkerGraphStructureStandardTest.class)
 * public class TinkerGraphStructureStandardTest implements GraphProvider {
 * }
 * </code> Implementing {@link GraphProvider} provides a way for the
 * {@code StructureStandardSuite} to instantiate {@link Graph} instances from
 * the implementation being tested to inject into tests in the suite. The
 * {@code StructureStandardSuite} will utilized Features defined in the suite to
 * determine which tests will be executed. Note that while the above example
 * demonstrates configuration of this suite, this approach generally applies to
 * all other test suites.
 * </p>
 * <p>
 * Set the {@code GREMLIN_TESTS} environment variable to a comma separated list
 * of test classes to execute. This setting can be helpful to restrict execution
 * of tests to specific ones being focused on during development.
 * </p>
 * 
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StructureTestSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed. Gremlin developers
     * should add to this list as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[] {
	    // CommunityGeneratorTest.class, DetachedGraphTest.class,
	    // DetachedEdgeTest.class, DetachedVertexPropertyTest.class,
	    // DetachedPropertyTest.class,
	    // DetachedVertexTest.class, DistributionGeneratorTest.class,
	    // EdgeTest.class, FeatureSupportTest.class, IoCustomTest.class,
	    // IoEdgeTest.class, IoGraphTest.class,
	    // IoVertexTest.class, IoPropertyTest.class,
	    // GraphTest.class, //
	    // GraphConstructionTest.class, //
	    // IoTest.class,
	    // VertexPropertyTest.class, VariablesTest.class,
	    // PropertyTest.class,
	    // ReferenceGraphTest.class, ReferenceEdgeTest.class,
	    // ReferenceVertexPropertyTest.class,
	    // ReferenceVertexTest.class, SerializationTest.class,
	    // StarGraphTest.class,
	    TransactionTest.class, //
	    // VertexTest.class //
    };

    public StructureTestSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
	super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }
}
