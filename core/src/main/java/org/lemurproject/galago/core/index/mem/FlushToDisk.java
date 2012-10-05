// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.lemurproject.galago.tupleflow.Utility;

public class FlushToDisk {

  private Logger logger = Logger.getLogger(FlushToDisk.class.toString());
  private MemoryIndex index;
  private String outputFolder;

  public void flushMemoryIndex(MemoryIndex index, String folder) throws IOException {
    flushMemoryIndex(index, folder, true);
  }

  public void flushMemoryIndex(MemoryIndex index, String folder, boolean threaded) throws IOException {
    this.index = index;
    this.outputFolder = folder;

    // first verify that there is at least one document
    //   and one term in the index
    if (index.documentsInIndex() < 1) {
      return;
    }

    if (new File(outputFolder).isDirectory()) {
      Utility.deleteDirectory(new File(outputFolder));
    }
    if (new File(outputFolder).isFile()) {
      new File(outputFolder).delete();
    }

    if (threaded) {
      flushThreaded(outputFolder);
    } else {
      flushLocal(outputFolder);
    }
  }

  private void flushLocal(String outputFolder) throws IOException {
    for (String partName : index.getPartNames()) {
      MemoryIndexPart part = index.getIndexPart(partName);
      try {
        part.flushToDisk(outputFolder + File.separator + partName);
      } catch (IOException e) {
        System.err.println("Failed to flush: " + partName);
        System.err.println(e.toString());
        e.printStackTrace();
      }
    }
  }

  private void flushThreaded(final String outputFolder) throws IOException {
    ArrayList<Thread> threads = new ArrayList();
    for (final String partName : index.getPartNames()) {
      final MemoryIndexPart part = index.getIndexPart(partName);
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            part.flushToDisk(outputFolder + File.separator + partName);
          } catch (IOException e) {
            System.err.println("Failed to flush: " + partName);
            System.err.println(e.toString());
            e.printStackTrace();
          }
        }
      };
      threads.add(t);
      t.start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException ex) {
        logger.severe(ex.getMessage());
      }
    }

  }
}
