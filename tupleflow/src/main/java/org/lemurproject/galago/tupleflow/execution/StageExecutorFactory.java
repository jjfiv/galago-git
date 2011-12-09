// BSD License (http://lemurproject.org/galago-license)
 
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.Utility;
import java.util.Arrays;

/**
 * Allows users to select some particular executor
 * Defaults to local executor
 *
 * @author trevor,sjh
 */
public class StageExecutorFactory {
    public static StageExecutor newInstance(String name, String... args) {
        if (name == null) {
            name = "local";
        }
        name = name.toLowerCase();

        if (name.startsWith("class=")) {
            String[] fields = name.split("=");
            assert fields.length >= 2;
            String className = fields[1];

            try {
                Class actual = Class.forName(className);
                return (StageExecutor) actual.newInstance();
            } catch (Exception e) {
                return null;
            }
        } else if (name.startsWith("thread")) {
            return new ThreadedStageExecutor();
        } else if (name.startsWith("ssh")) {
            return new SSHStageExecutor(args[0], Arrays.asList(Utility.subarray(args, 1)));
        } else if (name.equals("remotedebug")) {
            return new LocalRemoteStageExecutor();
        } else if (name.startsWith("drmaa")) {
            return new DRMAAStageExecutor(args);
        } else {
            return new LocalStageExecutor();
        }
    }
}
