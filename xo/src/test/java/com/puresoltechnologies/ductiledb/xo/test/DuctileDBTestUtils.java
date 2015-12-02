package com.puresoltechnologies.ductiledb.xo.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.BaseConfiguration;

import com.buschmais.xo.api.ConcurrencyMode;
import com.buschmais.xo.api.Transaction;
import com.buschmais.xo.api.ValidationMode;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.buschmais.xo.impl.bootstrap.XOUnitFactory;
import com.puresoltechnologies.ductiledb.core.GraphFactory;
import com.puresoltechnologies.ductiledb.core.StarWarsGraph;
import com.puresoltechnologies.ductiledb.core.core.core.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.xo.api.DuctileDBXOProvider;

/**
 * This class contains static methods which are test helpers for XO-Titan tests.
 * 
 * @author Rick-Rainer Ludwig
 * 
 */
public class DuctileDBTestUtils {

    private static final String XO_CONFIGURATION_RESOURCE = "/META-INF/xo.xml";

    /**
     * This is the default local URI for testing.
     */
    private static final URI DEFAULT_LOCAL_URI;

    static {
	try {
	    DEFAULT_LOCAL_URI = new URI("titan-cassandra://localhost:9160/titantest");
	} catch (URISyntaxException e) {
	    throw new RuntimeException(e);
	}
    }

    public static Collection<XOUnit[]> configuredXOUnits() throws IOException {
	List<XOUnit[]> xoUnits = new ArrayList<>();
	List<XOUnit> readXOUnits = XOUnitFactory.getInstance()
		.getXOUnits(AbstractXOTitanTest.class.getResource(XO_CONFIGURATION_RESOURCE));
	for (XOUnit xoUnit : readXOUnits) {
	    xoUnits.add(new XOUnit[] { xoUnit });
	}
	return xoUnits;
    }

    public static Collection<XOUnit[]> xoUnits() {
	return xoUnits(Arrays.asList(DEFAULT_LOCAL_URI), Collections.<Class<?>> emptyList(),
		Collections.<Class<?>> emptyList(), ValidationMode.AUTO, ConcurrencyMode.SINGLETHREADED,
		Transaction.TransactionAttribute.MANDATORY);
    }

    public static Collection<XOUnit[]> xoUnits(Class<?>... types) {
	return xoUnits(Arrays.asList(DEFAULT_LOCAL_URI), Arrays.asList(types), Collections.<Class<?>> emptyList(),
		ValidationMode.AUTO, ConcurrencyMode.SINGLETHREADED, Transaction.TransactionAttribute.MANDATORY);
    }

    public static Collection<XOUnit[]> xoUnits(List<URI> uris, List<? extends Class<?>> types) {
	return xoUnits(uris, types, Collections.<Class<?>> emptyList(), ValidationMode.AUTO,
		ConcurrencyMode.SINGLETHREADED, Transaction.TransactionAttribute.MANDATORY);
    }

    public static Collection<XOUnit[]> xoUnits(List<? extends Class<?>> types,
	    List<? extends Class<?>> instanceListeners, ValidationMode validationMode, ConcurrencyMode concurrencyMode,
	    Transaction.TransactionAttribute transactionAttribute) {
	return xoUnits(Arrays.asList(DEFAULT_LOCAL_URI), types, instanceListeners, validationMode, concurrencyMode,
		transactionAttribute);
    }

    public static Collection<XOUnit[]> xoUnits(List<URI> uris, List<? extends Class<?>> types,
	    List<? extends Class<?>> instanceListenerTypes, ValidationMode valiationMode,
	    ConcurrencyMode concurrencyMode, Transaction.TransactionAttribute transactionAttribute) {
	List<XOUnit[]> xoUnits = new ArrayList<>(uris.size());
	for (URI uri : uris) {
	    XOUnit xoUnit = new XOUnit("default", "Default XO unit", uri, DuctileDBXOProvider.class, new HashSet<>(types),
		    instanceListenerTypes, valiationMode, concurrencyMode, transactionAttribute, new Properties());
	    xoUnits.add(new XOUnit[] { xoUnit });
	}
	return xoUnits;
    }

    /**
     * This method adds the Starwars characters data into the Titan database for
     * testing purposes.
     * 
     * @param xoManager
     *            is the {@link XOManager} to be used.
     * @throws IOException
     */
    public static void addStarwarsData(XOManager xoManager) throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph(new BaseConfiguration())) {
	    StarWarsGraph.addStarWarsFiguresData(graph);
	}
    }
}
