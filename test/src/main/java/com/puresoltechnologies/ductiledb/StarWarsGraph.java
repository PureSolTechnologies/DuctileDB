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

    public static final String FIRST_NAME_PROPERTY = "FirstName";
    public static final String LAST_NAME_PROPERTY = "LastName";

    public static final String YETI_LABEL = "Yeti";
    public static final String MASTER_LABEL = "Master";
    public static final String ROBOT_LABEL = "Robot";
    public static final String PRINCESS_LABEL = "Princess";
    public static final String HAS_BROTHER_EDGE = "hasBrother";
    public static final String HAS_SISTER_EDGE = "hasSister";
    public static final String HAS_TRAINED_EDGE = "hasTrained";

    public static void addStarWarsFiguresData(DuctileDBGraph graph) {
	logger.info("Add Star Wars figures test data...");
	DuctileDBVertex lukeSkywalker = graph.addVertex("Luke Skywalker");
	lukeSkywalker.addLabel(YETI_LABEL);
	lukeSkywalker.setProperty(FIRST_NAME_PROPERTY, "Luke");
	lukeSkywalker.setProperty(LAST_NAME_PROPERTY, "Skywalker");

	DuctileDBVertex leiaOrgana = graph.addVertex("Leia Organa");
	leiaOrgana.addLabel(PRINCESS_LABEL);
	leiaOrgana.setProperty(FIRST_NAME_PROPERTY, "Leia");
	leiaOrgana.setProperty(LAST_NAME_PROPERTY, "Organa");

	DuctileDBVertex hanSolo = graph.addVertex("Han Solo");
	hanSolo.setProperty(FIRST_NAME_PROPERTY, "Han");
	hanSolo.setProperty(LAST_NAME_PROPERTY, "Solo");

	DuctileDBVertex bobaFett = graph.addVertex("Boba Fett");
	bobaFett.setProperty(FIRST_NAME_PROPERTY, "Boba");
	bobaFett.setProperty(LAST_NAME_PROPERTY, "Fett");

	DuctileDBVertex obiWanKenobi = graph.addVertex("Obi-Wan Kenobi");
	obiWanKenobi.addLabel(YETI_LABEL);
	obiWanKenobi.setProperty(FIRST_NAME_PROPERTY, "Obi-Wan");
	obiWanKenobi.setProperty(LAST_NAME_PROPERTY, "Kenobi");

	DuctileDBVertex r2D2 = graph.addVertex("R2-D2");
	r2D2.addLabel(ROBOT_LABEL);
	r2D2.setProperty(FIRST_NAME_PROPERTY, "R2-D2");

	DuctileDBVertex c3PO = graph.addVertex("C-3PO");
	c3PO.addLabel(ROBOT_LABEL);
	c3PO.setProperty(FIRST_NAME_PROPERTY, "C-3PO");

	DuctileDBVertex landoCalrissian = graph.addVertex("Lando Calrissian");
	landoCalrissian.setProperty(FIRST_NAME_PROPERTY, "Lando");
	landoCalrissian.setProperty(LAST_NAME_PROPERTY, "Calrissian");

	DuctileDBVertex quiGonJinn = graph.addVertex("Qui-Gon Jinn");
	quiGonJinn.addLabel(YETI_LABEL);
	quiGonJinn.addLabel(MASTER_LABEL);
	quiGonJinn.setProperty(FIRST_NAME_PROPERTY, "Qui-Gon");
	quiGonJinn.setProperty(LAST_NAME_PROPERTY, "Jinn");

	DuctileDBVertex maceWindu = graph.addVertex("Mace Windu");
	maceWindu.addLabel(YETI_LABEL);
	maceWindu.addLabel(MASTER_LABEL);
	maceWindu.setProperty(FIRST_NAME_PROPERTY, "Mace");
	maceWindu.setProperty(LAST_NAME_PROPERTY, "Windu");

	graph.addEdge(lukeSkywalker, leiaOrgana, HAS_SISTER_EDGE);
	graph.addEdge(leiaOrgana, lukeSkywalker, HAS_BROTHER_EDGE);
	graph.addEdge(quiGonJinn, obiWanKenobi, HAS_TRAINED_EDGE);

	graph.commit();
	logger.info("Star Wars figures test data added.");
    }

}
