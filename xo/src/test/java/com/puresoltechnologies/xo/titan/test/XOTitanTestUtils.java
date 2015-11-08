package com.puresoltechnologies.xo.titan.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.buschmais.xo.api.ConcurrencyMode;
import com.buschmais.xo.api.Transaction;
import com.buschmais.xo.api.ValidationMode;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.buschmais.xo.impl.bootstrap.XOUnitFactory;
import com.buschmais.xo.spi.metadata.type.TypeMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.puresoltechnologies.ductiledb.xo.api.TitanXOProvider;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStore;
import com.puresoltechnologies.xo.titan.test.data.TestData;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * This class contains static methods which are test helpers for XO-Titan tests.
 * 
 * @author Rick-Rainer Ludwig
 * 
 */
public class XOTitanTestUtils {

    private static final String XO_CONFIGURATION_RESOURCE = "/META-INF/xo.xml";

    /**
     * This is the default local URI for testing.
     */
    private static final URI DEFAULT_LOCAL_URI;
    static {
	try {
	    DEFAULT_LOCAL_URI = new URI(
		    "titan-cassandra://localhost:9160/titantest");
	} catch (URISyntaxException e) {
	    throw new RuntimeException(e);
	}
    }

    public static Collection<XOUnit[]> configuredXOUnits() throws IOException {
	List<XOUnit[]> xoUnits = new ArrayList<>();
	List<XOUnit> readXOUnits = XOUnitFactory.getInstance().getXOUnits(
		AbstractXOTitanTest.class
			.getResource(XO_CONFIGURATION_RESOURCE));
	for (XOUnit xoUnit : readXOUnits) {
	    xoUnits.add(new XOUnit[] { xoUnit });
	}
	return xoUnits;
    }

    public static Collection<XOUnit[]> xoUnits() {
	return xoUnits(Arrays.asList(DEFAULT_LOCAL_URI),
		Collections.<Class<?>> emptyList(),
		Collections.<Class<?>> emptyList(), ValidationMode.AUTO,
		ConcurrencyMode.SINGLETHREADED,
		Transaction.TransactionAttribute.MANDATORY);
    }

    public static Collection<XOUnit[]> xoUnits(Class<?>... types) {
	return xoUnits(Arrays.asList(DEFAULT_LOCAL_URI), Arrays.asList(types),
		Collections.<Class<?>> emptyList(), ValidationMode.AUTO,
		ConcurrencyMode.SINGLETHREADED,
		Transaction.TransactionAttribute.MANDATORY);
    }

    public static Collection<XOUnit[]> xoUnits(List<URI> uris,
	    List<? extends Class<?>> types) {
	return xoUnits(uris, types, Collections.<Class<?>> emptyList(),
		ValidationMode.AUTO, ConcurrencyMode.SINGLETHREADED,
		Transaction.TransactionAttribute.MANDATORY);
    }

    public static Collection<XOUnit[]> xoUnits(List<? extends Class<?>> types,
	    List<? extends Class<?>> instanceListeners,
	    ValidationMode validationMode, ConcurrencyMode concurrencyMode,
	    Transaction.TransactionAttribute transactionAttribute) {
	return xoUnits(Arrays.asList(DEFAULT_LOCAL_URI), types,
		instanceListeners, validationMode, concurrencyMode,
		transactionAttribute);
    }

    public static Collection<XOUnit[]> xoUnits(List<URI> uris,
	    List<? extends Class<?>> types,
	    List<? extends Class<?>> instanceListenerTypes,
	    ValidationMode valiationMode, ConcurrencyMode concurrencyMode,
	    Transaction.TransactionAttribute transactionAttribute) {
	List<XOUnit[]> xoUnits = new ArrayList<>(uris.size());
	for (URI uri : uris) {
	    XOUnit xoUnit = new XOUnit("default", "Default XO unit", uri,
		    TitanXOProvider.class, new HashSet<>(types),
		    instanceListenerTypes, valiationMode, concurrencyMode,
		    transactionAttribute, new Properties());
	    xoUnits.add(new XOUnit[] { xoUnit });
	}
	return xoUnits;
    }

    /**
     * This method is called if a keyspace for a given {@link XOUnit} is to be
     * cleared.
     * 
     * @param xoUnit
     *            is the {@link XOUnit} which points to the to be cleared
     *            keyspace.
     */
    public static void clearTitanKeyspace(XOUnit xoUnit) {
	Class<?> provider = xoUnit.getProvider();
	if (TitanXOProvider.class.equals(provider)) {
	    clearTitanKeyspace(xoUnit.getUri());
	}
    }

    /**
     * Clears the keyspaces assigned to the specified URI.
     * 
     * @param uri
     *            is an {@link URI} pointing to the keyspace to be cleaned.
     */
    private static void clearTitanKeyspace(URI uri) {
	String host = uri.getHost();
	int port = Integer.valueOf(uri.getPort());
	String keyspace = DuctileDBStore.retrieveKeyspaceFromURI(uri);
	DuctileDBStore titanCassandraStore = new DuctileDBStore(host,
		port, keyspace);
	try {
	    titanCassandraStore.init(new HashMap<Class<?>, TypeMetadata>());
	    TitanGraph titanGraph = titanCassandraStore.getTitanGraph();
	    Iterable<Vertex> vertices = titanGraph.query().vertices();
	    for (Vertex vertex : vertices) {
		vertex.remove();
	    }
	    titanGraph.commit();
	} finally {
	    titanCassandraStore.close();
	}
    }

    public static void dropTitanKeyspace(String host, String keyspace) {
	Cluster cluster = Cluster.builder().addContactPoint(host)
		.withPort(9042).build();
	try {
	    if (cluster.getMetadata().getKeyspace(keyspace) != null) {
		Session session = cluster.connect();
		try {
		    session.execute("DROP KEYSPACE " + keyspace + ";");
		} finally {
		    session.close();
		}
	    }
	} finally {
	    cluster.close();
	}
    }

    /**
     * Drops the whole keyspace for XO-Titan for a completely clean startup.
     * 
     * @param xoUnit
     *            is the {@link XOUnit} which points to the to be dropped
     *            keyspace.
     */
    public static void dropTitanKeyspace(XOUnit xoUnit) {
	Class<?> provider = xoUnit.getProvider();
	if (TitanXOProvider.class.equals(provider)) {
	    dropTitanKeyspace(xoUnit.getUri());
	}
    }

    /**
     * Drops the whole keyspace for XO-Titan for a completely clean startup.
     * 
     * @param uri
     *            is an {@link URI} pointing to the keyspace to be dropped.
     */
    private static void dropTitanKeyspace(URI uri) {
	String host = uri.getHost();
	int port = Integer.valueOf(9042);
	String keyspace = DuctileDBStore.retrieveKeyspaceFromURI(uri);

	try (Cluster cluster = Cluster.builder().addContactPoint(host)
		.withPort(port).build()) {
	    try (Session session = cluster.connect()) {
		session.execute("DROP KEYSPACE " + keyspace);
	    }
	}
    }

    /**
     * This method adds the Starwars characters data into the Titan database for
     * testing purposes.
     * 
     * @param xoManager
     *            is the {@link XOManager} to be used.
     */
    public static void addStarwarsData(XOManager xoManager) {
	TestData.addStarwars(xoManager);
    }
}
