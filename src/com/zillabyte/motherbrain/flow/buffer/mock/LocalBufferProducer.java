package com.zillabyte.motherbrain.flow.buffer.mock;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

import com.csvreader.CsvWriter;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.buffer.BufferProducer;
import com.zillabyte.motherbrain.flow.buffer.SinkToBuffer;
import com.zillabyte.motherbrain.flow.operations.OperationLogger;
import com.zillabyte.motherbrain.universe.Universe;

public class LocalBufferProducer implements BufferProducer {

  private SinkToBuffer _operation;
  private CsvWriter _csvOutput = null;

  public LocalBufferProducer(SinkToBuffer operation) {
    _operation = operation;
    if(Universe.instance().env().isLocal()) {
      String outputFile = Universe.instance().config().getOrException("output.prefix");
      if(!outputFile.equals("")) {
        outputFile += "_"+_operation.getTopicName()+".csv";
        try {
          String outputFilePath = Universe.instance().config().getOrException("directory")+"/"+outputFile;
          _operation.logger().writeLog("Writing output for relation ["+_operation.getTopicName()+"] to file: "+outputFilePath, OperationLogger.LogPriority.RUN);
          _csvOutput = new CsvWriter(new FileWriter(outputFilePath), ',');
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public synchronized void pushTuple(MapTuple t) {

    if(_csvOutput != null) {
      Map<String, Object> tupleValues = t.values();
      try {
        for(String key : tupleValues.keySet()) {
          _csvOutput.write(tupleValues.get(key).toString());
        }
        _csvOutput.endRecord();
        _csvOutput.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  public void closeFile() {
    if(_csvOutput != null) _csvOutput.close();
  }

}
