package com.puresoltechnologies.xo.titan.test.data;

import com.buschmais.xo.api.XOManager;

/**
 * This class provides some test data for testing.
 * 
 * @author Rick-Rainer Ludwig
 * 
 */
public class TestData {

	/**
	 * Adds the Starwars example data to the Titan database.
	 */
	public static void addStarwars(XOManager xoManager) {
		xoManager.currentTransaction().begin();

		Person padmeSkywalker = xoManager.create(Person.class);
		padmeSkywalker.setFirstName("Padme");
		padmeSkywalker.setLastName("Skywalker");

		Person anakinSkywalker = xoManager.create(Person.class);
		anakinSkywalker.setFirstName("Anakin");
		anakinSkywalker.setLastName("Skywalker");

		Person leaSkywalker = xoManager.create(Person.class);
		leaSkywalker.setFirstName("Lea");
		leaSkywalker.setLastName("Skywalker");
		leaSkywalker.setMother(padmeSkywalker);
		leaSkywalker.setFather(anakinSkywalker);

		Person lukeSkywalker = xoManager.create(Person.class);
		lukeSkywalker.setFirstName("Luke");
		lukeSkywalker.setLastName("Skywalker");
		lukeSkywalker.setMother(padmeSkywalker);
		lukeSkywalker.setFather(anakinSkywalker);

		xoManager.currentTransaction().commit();
	}
}
