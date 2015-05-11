package org.lemurproject.galago.utility.tools;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author jfoley.
 */
public class TarToZipConverter extends AppFunction {

  @Override
  public String getName() {
    return "tar-to-zip";
  }

  @Override
  public String getHelpString() {
    return makeHelpStr(
      "input", "the tar file",
      "output", "the zip file to write");
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    TarArchiveInputStream tais = new TarArchiveInputStream(StreamCreator.openInputStream(p.getString("input")));
    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(p.getString("output")));
    boolean quiet = p.get("quiet", false);

    while(true) {
      TarArchiveEntry tarEntry = tais.getNextTarEntry();
      if(tarEntry == null) break;
      if(!tarEntry.isFile()) continue;
      if(!tais.canReadEntryData(tarEntry)) continue;

      if(!quiet) System.err.println("# "+tarEntry.getName());

      ZipEntry forData = new ZipEntry(tarEntry.getName());
      forData.setSize(tarEntry.getSize());
      zos.putNextEntry(forData);
      StreamUtil.copyStream(tais, zos);
      zos.closeEntry();
    }
    tais.close();
    zos.close();
  }
}
