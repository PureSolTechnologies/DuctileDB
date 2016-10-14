package com.puresoltechnologies.ductiledb.core.graph;

import java.io.IOException;

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

    public static final String PERSON_TYPE = "Person";
    public static final String YETI_TYPE = "Yeti";
    public static final String MASTER_TYPE = "Master";
    public static final String ROBOT_TYPE = "Robot";
    public static final String PRINCESS_TYPE = "Princess";
    public static final String HAS_BROTHER_EDGE = "hasBrother";
    public static final String HAS_SISTER_EDGE = "hasSister";
    public static final String HAS_TRAINED_EDGE = "hasTrained";

    public static void addStarWarsFiguresData(GraphStore graph) throws IOException {
	logger.info("Add Star Wars figures test data...");
	DuctileDBVertex lukeSkywalker = graph.addVertex();
	lukeSkywalker.addType(PERSON_TYPE);
	lukeSkywalker.addType(YETI_TYPE);
	lukeSkywalker.setProperty(FIRST_NAME_PROPERTY, "Luke");
	lukeSkywalker.setProperty(LAST_NAME_PROPERTY, "Skywalker");

	DuctileDBVertex leiaOrgana = graph.addVertex();
	leiaOrgana.addType(PERSON_TYPE);
	leiaOrgana.addType(PRINCESS_TYPE);
	leiaOrgana.setProperty(FIRST_NAME_PROPERTY, "Leia");
	leiaOrgana.setProperty(LAST_NAME_PROPERTY, "Organa");

	DuctileDBVertex hanSolo = graph.addVertex();
	hanSolo.addType(PERSON_TYPE);
	hanSolo.setProperty(FIRST_NAME_PROPERTY, "Han");
	hanSolo.setProperty(LAST_NAME_PROPERTY, "Solo");

	DuctileDBVertex bobaFett = graph.addVertex();
	bobaFett.addType(PERSON_TYPE);
	bobaFett.setProperty(FIRST_NAME_PROPERTY, "Boba");
	bobaFett.setProperty(LAST_NAME_PROPERTY, "Fett");

	DuctileDBVertex obiWanKenobi = graph.addVertex();
	obiWanKenobi.addType(PERSON_TYPE);
	obiWanKenobi.addType(YETI_TYPE);
	obiWanKenobi.setProperty(FIRST_NAME_PROPERTY, "Obi-Wan");
	obiWanKenobi.setProperty(LAST_NAME_PROPERTY, "Kenobi");

	DuctileDBVertex r2D2 = graph.addVertex();
	r2D2.addType(ROBOT_TYPE);
	r2D2.setProperty(FIRST_NAME_PROPERTY, "R2-D2");

	DuctileDBVertex c3PO = graph.addVertex();
	c3PO.addType(ROBOT_TYPE);
	c3PO.setProperty(FIRST_NAME_PROPERTY, "C-3PO");

	DuctileDBVertex landoCalrissian = graph.addVertex();
	landoCalrissian.addType(PERSON_TYPE);
	landoCalrissian.setProperty(FIRST_NAME_PROPERTY, "Lando");
	landoCalrissian.setProperty(LAST_NAME_PROPERTY, "Calrissian");

	DuctileDBVertex quiGonJinn = graph.addVertex();
	quiGonJinn.addType(PERSON_TYPE);
	quiGonJinn.addType(YETI_TYPE);
	quiGonJinn.addType(MASTER_TYPE);
	quiGonJinn.setProperty(FIRST_NAME_PROPERTY, "Qui-Gon");
	quiGonJinn.setProperty(LAST_NAME_PROPERTY, "Jinn");

	DuctileDBVertex maceWindu = graph.addVertex();
	maceWindu.addType(PERSON_TYPE);
	maceWindu.addType(YETI_TYPE);
	maceWindu.addType(MASTER_TYPE);
	maceWindu.setProperty(FIRST_NAME_PROPERTY, "Mace");
	maceWindu.setProperty(LAST_NAME_PROPERTY, "Windu");

	graph.addEdge(lukeSkywalker, leiaOrgana, HAS_SISTER_EDGE);
	graph.addEdge(leiaOrgana, lukeSkywalker, HAS_BROTHER_EDGE);
	graph.addEdge(quiGonJinn, obiWanKenobi, HAS_TRAINED_EDGE);

	graph.commit();
	logger.info("Star Wars figures test data added.");
    }

}
