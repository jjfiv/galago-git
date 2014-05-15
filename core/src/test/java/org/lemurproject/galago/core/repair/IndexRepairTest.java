package org.lemurproject.galago.core.repair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.io.IOException;

public class IndexRepairTest {
  private File tempPath;

  @Before
  public void setUp() throws IOException, IncompatibleProcessorException {
    this.tempPath = LocalRetrievalTest.makeIndex();
  }

  @After
  public void tearDown() throws IOException {
    Utility.deleteDirectory(tempPath);
  }

  @Test
  public void testRepairNamesReverse() throws IOException, IncompatibleProcessorException {
    String namesPart = (new File(tempPath, "names")).getCanonicalPath();
    String originalRNames = (new File(tempPath, "names.reverse")).getCanonicalPath();
    File tempPart = null;

    try {
      tempPart = FileUtility.createTemporary();
      IndexRepair.createNamesReverseFromNames(namesPart, tempPart.getAbsolutePath());

      DiskNameReverseReader original = new DiskNameReverseReader(originalRNames);
      DiskNameReverseReader recreated = new DiskNameReverseReader(tempPart.getAbsolutePath());

      DiskNameReverseReader.KeyIterator iter = original.getIterator();
      while(!iter.isDone()) {
        long id = iter.getCurrentIdentifier();
        String name = iter.getCurrentName();
        Assert.assertEquals(recreated.getDocumentIdentifier(name), id);
        iter.nextKey();
      }

    } finally {
      if(tempPart != null) tempPart.delete();
    }
  }
}