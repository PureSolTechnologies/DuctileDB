package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.engine.EngineChecks;

public class EngineChecksTest {

    @Test
    public void testValidIdentifiers() {
	assertTrue(EngineChecks.checkIdentifier("a"));
	assertTrue(EngineChecks.checkIdentifier("A"));
	assertTrue(EngineChecks.checkIdentifier("z"));
	assertTrue(EngineChecks.checkIdentifier("Z"));
	assertTrue(EngineChecks.checkIdentifier("abcdefghijklmnopqrstuvwxyz"));
	assertTrue(EngineChecks.checkIdentifier("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
	assertTrue(EngineChecks.checkIdentifier("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"));
    }

    @Test
    public void testInvalidIdentifiers() {
	assertFalse(EngineChecks.checkIdentifier(""));
	assertFalse(EngineChecks.checkIdentifier(null));
	assertFalse(EngineChecks.checkIdentifier("-a"));
	assertFalse(EngineChecks.checkIdentifier("_a"));
	assertFalse(EngineChecks.checkIdentifier("0a"));
    }

}
