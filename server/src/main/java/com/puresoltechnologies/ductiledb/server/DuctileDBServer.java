package com.puresoltechnologies.ductiledb.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.puresoltechnologies.ductiledb.commons.Metrics;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class DuctileDBServer extends Application<DuctileDBServerConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBServer.class);

    @Override
    public String getName() {
	return DuctileDBServer.class.getSimpleName();
    }

    @Override
    public void initialize(Bootstrap<DuctileDBServerConfiguration> bootstrap) {
	bootstrap.addBundle(new AssetsBundle("/ui", "", "index.html"));
    }

    @Override
    public void run(DuctileDBServerConfiguration configuration, Environment environment) throws Exception {
	MetricRegistry metrics = environment.metrics();
	Metrics.initialize(metrics);

	HealthCheckRegistry healthChecks = environment.healthChecks();

	JerseyEnvironment jersey = environment.jersey();
	jersey.setUrlPattern("/rest");

    }

    @Override
    protected void onFatalError() {
	logger.error("SEVERE ISSUE OCCURED. APPLICATION IS SHUTTING DOWN.");
	super.onFatalError();
    }

    public static void main(String[] args) {
	try {
	    DuctileDBServer application = new DuctileDBServer();
	    application.run(args);
	} catch (Throwable e) {
	    logger.error("SEVERE ISSUE OCCURED. APPLICATION IS SHUTTING DOWN.", e);
	    System.exit(1);
	}
    }

}
