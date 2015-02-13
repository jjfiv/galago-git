// BSD License (http://lemurproject.org/galago-license)
 
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.reflection.ReflectUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * Allows users to select some particular executor
 * Defaults to local executor
 *
 * @author trevor,sjh
 */
public class StageExecutorFactory {
    /** lazyily-init reference to GridEngine executor if it exists on classpath */
    protected static Constructor<? extends StageExecutor> drmaaExecutor = null;

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
        } else if (name.startsWith("drmaa")) {
            // Cache this constructor statically for further calls.
            if(drmaaExecutor == null) {
                try {
                    drmaaExecutor = ReflectUtil.getConstructor("org.lemurproject.galago.tupleflow.execution.DRMAAStageExecutor", String[].class);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException("Sorry, you need tupleflow-gridengine on the class path to use mode=drmaa.", e);
                }
            }
            // Instantiate via reflection or die.
            try {
                return drmaaExecutor.newInstance(args);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Couldn't instantiate DRMAAStageExecutor with args="+ Arrays.toString(args), e);
            }
        } else {
            return new LocalCheckpointedStageExecutor();
        }
    }
}
