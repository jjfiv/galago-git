// BSD License (http://lemurproject.org/galago-license)
 
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.forkmode.ForkModeStageExecutor;
import org.lemurproject.galago.tupleflow.slurm.SlurmModeStageExecutor;

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
            return new ThreadedCheckpointedStageExecutor();
        } else if (name.startsWith("ssh")) {
            return new SSHStageExecutor(args[0], Arrays.asList(Utility.subarray(args, 1)));
        } else if (name.equals("remotedebug")) {
            return new LocalCheckpointedStageExecutor();
        } else if (name.equals("fork")) {
            return new ForkModeStageExecutor(args);
        } else if (name.equals("slurm")) {
            return new SlurmModeStageExecutor(args);
        } else if (name.startsWith("drmaa")) {
              throw new IllegalArgumentException("Sorry, in order to use mode=drmaa, you'll have to use a version of Galago less than 3.16");
        } else {
            return new LocalCheckpointedStageExecutor();
        }
    }
}
