// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.tupleflow.execution.StageInstanceDescription.PipeInput;
import org.lemurproject.galago.tupleflow.execution.StageInstanceDescription.PipeOutput;

/**
 *
 * @author trevor
 */
public class StageGroupDescription {
    /// A stage object (probably genereated by the JobConstructor parser)
    Stage stage;
    
    public HashMap<String, DataPipeRegion> inputs;
    public HashMap<String, DataPipeRegion> outputs;
    
    /// Count of instances of this stage
    public int instanceCount;

    /// URL of the Master for this job.
    String masterURL;
    
    public static class DataPipeRegion {
        DataPipe pipe;
        int start;
        int end;
        ConnectionPointType direction;
        
        public DataPipeRegion(DataPipe pipe, int start, int end, ConnectionPointType direction) {
            this.pipe = pipe;
            this.start = start;
            this.end = end;
            this.direction = direction;
        }
        
        public int fileCount() {
            int count = 0;
            
            for(int i=start; i<end; i++) {
                String[] filenames = pipe.getOutputFileNames(i);
                count += filenames.length;
            }
            
            return count;
        }
    }
    
    public StageGroupDescription(Stage stage) {
        this(stage, 1, "");
    }
    
    /** Creates a new instance of StageGroupDescription */
    public StageGroupDescription(Stage stage, int instanceCount, String masterURL) {
        this.stage = stage;
        this.inputs = new HashMap<String, DataPipeRegion>();
        this.outputs = new HashMap<String, DataPipeRegion>();
        this.instanceCount = instanceCount;
        this.masterURL = masterURL;
    }
    
    public String getName() {
        return stage.name;
    }

    public String getMasterURL() {
        return masterURL;
    }

    public void setMasterURL(String masterURL) {
        this.masterURL = masterURL;
    }

    public boolean containsInput(String name) {
        return inputs.containsKey(name);
    }
    
    public boolean containsOutput(String name) {
        return outputs.containsKey(name);
    }

    public Stage getStage() {
        return stage;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    @Override
    public String toString() {
        return stage.toString();
    }


    public List<StageInstanceDescription> getInstances() {
        ArrayList<StageInstanceDescription> instances = new ArrayList();
        
        for(int i=0; i<instanceCount; i++) {
            Map<String, PipeInput> instanceOutputs = new HashMap<String, PipeInput>();
            
            for(String key : outputs.keySet()) {
                DataPipeRegion region = outputs.get(key);
                
                if(region.end - region.start <= 1) {
                    instanceOutputs.put(key, new PipeInput(region.pipe, 0));
                } else {
                    assert region.end - region.start == instanceCount;
                    instanceOutputs.put(key, new PipeInput(region.pipe, region.start + i));
                }
            }
            
            Map<String, PipeOutput> instanceInputs = new HashMap<String, PipeOutput>();

            for(String key : inputs.keySet()) {
                DataPipeRegion region = inputs.get(key);
                
                if(region.end - region.start <= 1) {
                    instanceInputs.put(key, new PipeOutput(region.pipe, 0));
                } else if(instanceCount == 1 && region.end - region.start > 1) {
                    // assignment == "combined"
                    instanceInputs.put(key, new PipeOutput(region.pipe, region.start, region.end));
                } else {
                    assert region.end - region.start == instanceCount;
                    instanceInputs.put(key, new PipeOutput(region.pipe, region.start + i));
                }
            }

            StageInstanceDescription instance =
                    new StageInstanceDescription(
                            stage, i, instanceOutputs, instanceInputs, masterURL);
            instances.add(instance);
        }
        
        return instances;
    }
}
