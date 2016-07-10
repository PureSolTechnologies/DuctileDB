package com.puresoltechnologies.ductiledb.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphFactory;

/**
 * This is the central factory to connect to DuctileDB. The primary goal is to
 * have a simple point of access, because DuctileDB needs some information to
 * connect to Hadoop and HBase.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBFactory {

    public static Configuration createConfiguration(File hadoopHome, File hbaseHome) throws FileNotFoundException {
	checkDirectoryExistence("Hadoop home", hadoopHome);
	checkDirectoryExistence("HBase home", hbaseHome);
	// HBase configuration
	Configuration hbaseConfiguration = HBaseConfiguration.create();
	File hbaseSiteFile = getHBaseSiteFile(hbaseHome);
	hbaseConfiguration.addResource(new Path(hbaseSiteFile.getPath()));
	// Hadoop configuration
	File hadoopConfigDirectory = getHadoopConfigurationDirectory(hadoopHome);
	File coreSiteFile = getHadoopCoreSiteFile(hadoopConfigDirectory);
	File hdfsSiteFile = getHadoopHDFSSiteFile(hadoopConfigDirectory);
	File mapredSiteFile = getHadoopMapRedSiteFile(hadoopConfigDirectory);
	hbaseConfiguration.addResource(new Path(coreSiteFile.getPath()));
	hbaseConfiguration.addResource(new Path(hdfsSiteFile.getPath()));
	hbaseConfiguration.addResource(new Path(mapredSiteFile.getPath()));
	return hbaseConfiguration;
    }

    private static void checkDirectoryExistence(String name, File directory) throws FileNotFoundException {
	if (!directory.exists()) {
	    throw new FileNotFoundException(name + " '" + directory.getPath() + "' does not exist.");
	}
	if (!directory.isDirectory()) {
	    throw new FileNotFoundException(name + " '" + directory.getPath() + "' is not a directory.");
	}
    }

    private static void checkFileExistence(String name, File file) throws FileNotFoundException {
	if (!file.exists()) {
	    throw new FileNotFoundException(name + " '" + file.getPath() + "' does not exist.");
	}
	if (!file.isFile()) {
	    throw new FileNotFoundException(name + " '" + file.getPath() + "' is not a file.");
	}
    }

    private static File getHadoopMapRedSiteFile(File hadoopConfigDirectory) throws FileNotFoundException {
	File mapredSiteFile = new File(hadoopConfigDirectory, "mapred-site.xml");
	checkFileExistence("mapred-site.xml", mapredSiteFile);
	return mapredSiteFile;
    }

    private static File getHadoopHDFSSiteFile(File hadoopConfigDirectory) throws FileNotFoundException {
	File hdfsSiteFile = new File(hadoopConfigDirectory, "hdfs-site.xml");
	checkFileExistence("hdfs-site.xml", hdfsSiteFile);
	return hdfsSiteFile;
    }

    private static File getHadoopCoreSiteFile(File hadoopConfigDirectory) throws FileNotFoundException {
	File coreSiteFile = new File(hadoopConfigDirectory, "core-site.xml");
	checkFileExistence("core-site.xml", coreSiteFile);
	return coreSiteFile;
    }

    private static File getHadoopConfigurationDirectory(File hadoopHome) throws FileNotFoundException {
	File hadoopConfigDirectory = new File(new File(hadoopHome, "etc"), "hadoop");
	checkDirectoryExistence("Hadoop configuration directory", hadoopConfigDirectory);
	return hadoopConfigDirectory;
    }

    private static File getHBaseSiteFile(File hbaseHome) throws FileNotFoundException {
	File hbaseSiteFile = new File(new File(hbaseHome, "conf"), "hbase-site.xml");
	checkFileExistence("hbase-site.xml", hbaseSiteFile);
	return hbaseSiteFile;
    }

    public static DuctileDB connect(File hadoopHome, File hbaseHome)
	    throws FileNotFoundException, IOException, ServiceException {
	return connect(createConfiguration(hadoopHome, hbaseHome));
    }

    public static DuctileDB connect(Configuration configuration) throws IOException, ServiceException {
	BlobStore blobStore = new BlobStoreImpl(configuration);
	DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(configuration);
	return new DuctileDBImpl(blobStore, graph);
    }

}
