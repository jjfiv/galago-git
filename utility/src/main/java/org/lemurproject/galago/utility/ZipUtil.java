// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * @author jfoley
 */
public class ZipUtil {
  public static List<String> listZipFile(ZipFile zipFile) throws IOException {
    ArrayList<String> results = new ArrayList<>();

    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    while (entries.hasMoreElements()) {
      ZipEntry zipEntry = entries.nextElement();
      if (zipEntry.isDirectory()) {
        continue;
      }
      results.add(zipEntry.getName());
    }

    return results;
  }

  public static InputStream streamZipEntry(ZipFile zipF, String entry) throws IOException {
    return zipF.getInputStream(zipF.getEntry(entry));
  }

  public static BufferedReader readZipEntry(ZipFile zipF, String entry) throws IOException {
    return new BufferedReader(new InputStreamReader(streamZipEntry(zipF, entry), "UTF-8"));
  }

  public static boolean hasZipExtension(String path) {
    if(path.endsWith(".zip")) {
      return true;
    }
    return false;
  }

  public static void write(ZipOutputStream zos, String name, String data) throws IOException {
    write(zos, name, ByteUtil.fromString(data));
  }
  public static void write(ZipOutputStream zos, String name, byte[] data) throws IOException {
    ZipEntry forData = new ZipEntry(name);
    forData.setSize(data.length);
    zos.putNextEntry(forData);
    zos.write(data);
    zos.closeEntry();
  }

  public static ZipFile open(File zipFile) throws IOException {
    return new ZipFile(zipFile);
  }

  public static ZipFile open(String fileName) throws IOException {
    return new ZipFile(fileName);
  }
}
