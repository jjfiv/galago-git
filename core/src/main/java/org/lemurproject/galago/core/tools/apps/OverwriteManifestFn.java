/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.io.RandomAccessFile;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class OverwriteManifestFn extends AppFunction {

  @Override
  public String getName() {
    return "overwrite-manifest";
  }

  @Override
  public String getHelpString() {
    return "galago overwrite-manifest --indexPath=/path/to/index/file --key=value\n"
            + "  Rewrites internal index manifest data for index file.\n"
            + "  Allows parameters to be changed after index files have been written.\n\n"
            + "  WARNING : Use with caution - changing some parameters may make the index file non-readable.\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // first open the index
    String filename = p.getString("indexPath");
    RandomAccessFile indexReaderWriter = new RandomAccessFile(filename, "rw");

    long length = indexReaderWriter.length();
    long footerOffset = length - Integer.SIZE / 8 - 3 * Long.SIZE / 8;

    indexReaderWriter.seek(footerOffset);

    // read metadata values:
    long vocabularyOffset = indexReaderWriter.readLong();
    long manifestOffset = indexReaderWriter.readLong();
    int blockSize = indexReaderWriter.readInt();
    long magicNumber = indexReaderWriter.readLong();

    indexReaderWriter.seek(manifestOffset);
    byte[] xmlData = new byte[(int) (footerOffset - manifestOffset)];
    indexReaderWriter.read(xmlData);
    Parameters newParameters = Parameters.parse(xmlData);
    newParameters.copyFrom(p);

    indexReaderWriter.seek(manifestOffset);

    // write the new data back to the file
    xmlData = newParameters.toString().getBytes("UTF-8");
    indexReaderWriter.write(xmlData);
    indexReaderWriter.writeLong(vocabularyOffset);
    indexReaderWriter.writeLong(manifestOffset);
    indexReaderWriter.writeInt(blockSize);
    indexReaderWriter.writeLong(magicNumber);
    indexReaderWriter.close();
  }
}
