package com.puresoltechnologies.xo.titan.test.filesystem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.ResultIterable;
import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class FileSystemIT extends AbstractXOTitanTest {

    public FileSystemIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
	return XOTitanTestUtils.xoUnits(Directory.class, File.class);
    }

    @Test
    public void fileSystemTree() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();

	Directory root = xoManager.create(Directory.class);
	root.setName("rootdir");
	Directory subDirectory = xoManager.create(Directory.class);
	subDirectory.setName("subdir");

	File rootFile1 = xoManager.create(File.class);
	rootFile1.setName("rootfile1");
	File rootFile2 = xoManager.create(File.class);
	rootFile2.setName("rootfile2");
	File subDirFile1 = xoManager.create(File.class);
	subDirFile1.setName("subdirfile1");
	File subDirFile2 = xoManager.create(File.class);
	subDirFile2.setName("subdirfile2");

	root.getDirectories().add(subDirectory);
	root.getFiles().add(rootFile1);
	root.getFiles().add(rootFile2);

	subDirectory.getFiles().add(subDirFile1);
	subDirectory.getFiles().add(subDirFile2);

	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	ResultIterable<Directory> fsRootResultIterable = xoManager.find(
		Directory.class, "rootdir");
	assertTrue(fsRootResultIterable.hasResult());
	ResultIterator<Directory> fsRootIterator = fsRootResultIterable
		.iterator();
	assertTrue(fsRootIterator.hasNext());
	Directory fsRoot = fsRootIterator.next();
	assertFalse(fsRootIterator.hasNext());
	printFilesystem(fsRoot, 0);
	xoManager.currentTransaction().commit();
    }

    private void printFilesystem(Directory fsNode, int depth) {
	intent(depth);
	System.out.print("/");
	System.out.print(fsNode.getName());
	System.out.println("/");
	depth++;
	for (File file : fsNode.getFiles()) {
	    intent(depth);
	    System.out.println(file.getName());
	}
	for (Directory directory : fsNode.getDirectories()) {
	    printFilesystem(directory, depth);
	}
    }

    private void intent(int depth) {
	for (int i = 0; i < depth * 4; i++) {
	    System.out.print(' ');
	}
    }

}
