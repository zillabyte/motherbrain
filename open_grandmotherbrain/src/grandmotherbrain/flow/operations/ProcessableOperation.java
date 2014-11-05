package grandmotherbrain.flow.operations;

import grandmotherbrain.flow.MapTuple;
import grandmotherbrain.flow.collectors.OutputCollector;

public interface ProcessableOperation {
  public void handleProcess(MapTuple t, OutputCollector c) throws Exception;
}
