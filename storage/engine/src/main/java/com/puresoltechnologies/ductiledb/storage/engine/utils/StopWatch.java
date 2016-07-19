package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.time.Duration;
import java.time.Instant;

/**
 * A simple stop watch measure times.
 * 
 * @author Rick-Rainer Ludwig
 */
public class StopWatch {

    private Instant start;
    private Instant stop;

    public void start() {
	start = Instant.now();
    }

    public void stop() {
	stop = Instant.now();
    }

    public Duration getDuration() {
	return Duration.between(start, stop);
    }

    public long getMillis() {
	return getDuration().toMillis();
    }
}
