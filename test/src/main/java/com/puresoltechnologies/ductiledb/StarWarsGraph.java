package com.puresoltechnologies.ductiledb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to provide sample data for testing. The sample data
 * consists of Star Wars figures and their relations to each other.
 * 
 * @author Rick-Rainer Ludwig
 */
public class StarWarsGraph {

    private static final Logger logger = LoggerFactory.getLogger(StarWarsGraph.class);

    public static void addStarWarsFiguresData(DuctileDBGraph graph) {
	logger.info("Add Star Wars figures test data...");
	DuctileDBVertex lukeSkywalker = graph.addVertex("Luke Skywalker");
	lukeSkywalker.addLabel("Yeti");
	lukeSkywalker.setProperty("FirstName", "Luke");
	lukeSkywalker.setProperty("LastName", "Skywalker");

	DuctileDBVertex leiaOrgana = graph.addVertex("Leia Organa");
	leiaOrgana.addLabel("Princess");
	leiaOrgana.setProperty("FirstName", "Leia");
	leiaOrgana.setProperty("LastName", "Organa");

	DuctileDBVertex hanSolo = graph.addVertex("Han Solo");
	hanSolo.setProperty("FirstName", "Han");
	hanSolo.setProperty("LastName", "Solo");

	DuctileDBVertex bobaFett = graph.addVertex("Boba Fett");
	bobaFett.setProperty("FirstName", "Boba");
	bobaFett.setProperty("LastName", "Fett");

	DuctileDBVertex obiWanKenobi = graph.addVertex("Obi-Wan Kenobi");
	obiWanKenobi.addLabel("Yeti");
	obiWanKenobi.setProperty("FirstName", "Obi-Wan");
	obiWanKenobi.setProperty("LastName", "Kenobi");

	DuctileDBVertex r2D2 = graph.addVertex("R2-D2");
	r2D2.addLabel("Robot");
	r2D2.setProperty("FirstName", "R2-D2");

	DuctileDBVertex c3PO = graph.addVertex("C-3PO");
	c3PO.addLabel("Robot");
	c3PO.setProperty("FirstName", "C-3PO");

	DuctileDBVertex landoCalrissian = graph.addVertex("Lando Calrissian");
	landoCalrissian.setProperty("FirstName", "Lando");
	landoCalrissian.setProperty("LastName", "Calrissian");

	DuctileDBVertex quiGonJinn = graph.addVertex("Qui-Gon Jinn");
	quiGonJinn.addLabel("Yeti");
	quiGonJinn.addLabel("Master");
	quiGonJinn.setProperty("FirstName", "Qui-Gon");
	quiGonJinn.setProperty("LastName", "Jinn");

	DuctileDBVertex maceWindu = graph.addVertex("Mace Windu");
	maceWindu.addLabel("Yeti");
	maceWindu.addLabel("Master");
	maceWindu.setProperty("FirstName", "Mace");
	maceWindu.setProperty("LastName", "Windu");

	graph.commit();
	logger.info("Star Wars figures test data added.");
    }

}
