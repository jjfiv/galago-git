package org.lemurproject.galago.utility;

import org.junit.Test;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.StreamUtil;
import org.lemurproject.galago.utility.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZipUtilTest {

    @Test
    public void testZipFile() throws IOException {
        File tmp = null;

        try {
            tmp = File.createTempFile("zipUtilTest", ".zip");

            String fooContents = "foo is the best";
            String fooPath = "data/foo.txt";

            String barContents = "bar is the best";
            String barPath = "data/subdir/ignore/bar.txt";

            // write zip file:
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tmp.getAbsolutePath()));
            ZipUtil.write(zos, fooPath, ByteUtil.fromString(fooContents));
            ZipUtil.write(zos, barPath, ByteUtil.fromString(barContents));
            zos.close();

            ZipFile zipFile = ZipUtil.open(tmp);

            // read zip file:
            List<String> entries = ZipUtil.listZipFile(zipFile);
            assertEquals(2, entries.size());
            assertEquals(fooPath, entries.get(0));
            assertEquals(barPath, entries.get(1));

            assertEquals(fooContents, StreamUtil.copyStreamToString(ZipUtil.streamZipEntry(zipFile, fooPath)));
            assertEquals(barContents, StreamUtil.copyStreamToString(ZipUtil.streamZipEntry(zipFile, barPath)));

            assertTrue(ZipUtil.hasZipExtension(tmp.getAbsolutePath()));

        } finally {
            if (tmp != null) {
                tmp.delete();
            }
        }

    }
}
