/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.junit.Assert;
import org.junit.Test;
import org.lemurproject.galago.contrib.index.disk.BackgroundStatsReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.lemurproject.galago.contrib.util.TestingUtils.trecDocument;

/**
 *
 * @author sjh
 */
public class BuildSpecialCollBackgroundTest {

    @Test
    public void testSomeMethod() throws Exception {
        File docs = FileUtility.createTemporary();
        File back = FileUtility.createTemporary();
        File index = FileUtility.createTemporaryDirectory();
        try {
            makeTrecDocs(docs);

            Parameters p = Parameters.instance();
            p.set("inputPath", docs.getAbsolutePath());
            p.set("indexPath", index.getAbsolutePath());
            p.set("corpus", false);
            App.run("build", p, System.out);

            Utility.copyStringToFile(
                    "1\t1\n"
                    + "2\t2\n"
                    + "3\t3\n"
                    + "4\t4\n"
                    + "4\t4\n", back);

            Parameters p2 = Parameters.instance();
            p2.set("inputPath", back.getAbsolutePath());
            p2.set("indexPath", index.getAbsolutePath());
            p2.set("partName", "back");
            App.run("build-special-coll-background", p2, System.out);

            BackgroundStatsReader backPart = (BackgroundStatsReader) DiskIndex.openIndexComponent(new File(index, "back").getAbsolutePath());
            assertEquals(backPart.getManifest().getLong("statistics/highestCollectionFrequency"), 8);

            Retrieval r = RetrievalFactory.instance(index.getAbsolutePath(), Parameters.instance());
            assertEquals(r.getNodeStatistics("#counts:@/0/:part=back()").nodeFrequency, 0);
            assertEquals(r.getNodeStatistics("#counts:@/4/:part=back()").nodeFrequency, 8);

            backPart.close();
            r.close();
            p2.clear();
            p.clear();

        } finally {

            Assert.assertTrue(docs.delete());
            Assert.assertTrue(back.delete());
            Utility.deleteDirectory(index);
        }
    }

    private void makeTrecDocs(File input) throws Exception {
        Random r = new Random();
        StringBuilder corpus = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            StringBuilder text = new StringBuilder();
            for (int j = 0; j < 100; j++) {
                text.append(" ").append(r.nextInt(100));
            }
            corpus.append(trecDocument("doc-" + i, text.toString()));
        }
        Utility.copyStringToFile(corpus.toString(), input);

    }
}
