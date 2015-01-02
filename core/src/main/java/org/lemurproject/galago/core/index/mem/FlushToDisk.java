// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.utility.FSUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

public class FlushToDisk {

  private static Logger logger = Logger.getLogger(FlushToDisk.class.toString());

  public static void flushMemoryIndex(MemoryIndex index, String folder) throws IOException {
    flushMemoryIndex(index, folder, true);
  }

  public static void flushMemoryIndex(MemoryIndex index, String folder, boolean threaded) throws IOException {
    String outputFolder = folder;

    // first verify that there is at least one document
    //   and one term in the index
    if (index.documentsInIndex() < 1) {
      return;
    }

    if (new File(outputFolder).isDirectory()) {
      FSUtil.deleteDirectory(new File(outputFolder));
    }
    if (new File(outputFolder).isFile()) {
      new File(outputFolder).delete();
    }

    if (threaded) {
      flushThreaded(index, outputFolder);
    } else {
      flushLocal(index, outputFolder);
    }
  }

  private static void flushLocal(MemoryIndex index, String outputFolder) throws IOException {
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

  private static void flushThreaded(MemoryIndex index, final String outputFolder) throws IOException {
    ArrayList<Thread> threads = new ArrayList<>();
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
