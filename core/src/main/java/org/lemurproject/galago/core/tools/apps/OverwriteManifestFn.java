/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;

import java.io.PrintStream;
import java.io.RandomAccessFile;

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
    RandomAccessFile indexReaderWriter = StreamCreator.writeFile(filename);

    long length = indexReaderWriter.length();
    long footerOffset = length - Integer.SIZE / 8 - 3 * Long.SIZE / 8;

    indexReaderWriter.seek(footerOffset);

    // read metadata values:
    long vocabularyOffset = indexReaderWriter.readLong();
    long manifestOffset = indexReaderWriter.readLong();
    int blockSize = indexReaderWriter.readInt();
    long magicNumber = indexReaderWriter.readLong();

    indexReaderWriter.seek(manifestOffset);
    byte[] manifestData = new byte[(int) (footerOffset - manifestOffset)];
    indexReaderWriter.read(manifestData);
    Parameters newParameters = Parameters.parseBytes(manifestData);
    newParameters.copyFrom(p);

    indexReaderWriter.seek(manifestOffset);

    // write the new data back to the file
    manifestData = newParameters.toString().getBytes("UTF-8");
    indexReaderWriter.write(manifestData);
    indexReaderWriter.writeLong(vocabularyOffset);
    indexReaderWriter.writeLong(manifestOffset);
    indexReaderWriter.writeInt(blockSize);
    indexReaderWriter.writeLong(magicNumber);
    indexReaderWriter.close();
  }
}
