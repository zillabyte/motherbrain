package com.zillabyte.motherbrain.flow.components.builtin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.Response;
import com.zillabyte.motherbrain.flow.Component;
import com.zillabyte.motherbrain.flow.MapTuple;
import com.zillabyte.motherbrain.flow.StreamBuilder.ComponentStreamBuilder;
import com.zillabyte.motherbrain.flow.collectors.OutputCollector;
import com.zillabyte.motherbrain.flow.components.ComponentInput;
import com.zillabyte.motherbrain.flow.components.ComponentOutput;
import com.zillabyte.motherbrain.flow.config.FlowConfig;
import com.zillabyte.motherbrain.flow.operations.OperationException;
import com.zillabyte.motherbrain.flow.operations.builtin.Clumper;
import com.zillabyte.motherbrain.relational.ColumnDef;
import com.zillabyte.motherbrain.utils.MeteredLog;
import com.zillabyte.motherbrain.utils.Utils;

public class FetchUrlComponent {

  public static class Handler extends Clumper {
    private static final long serialVersionUID = 8362569227165803567L;
    private transient AsyncHttpClient _asyncHttpClient;

    public Handler(String name, int clumpCount) {
      super(name, clumpCount);
    }

    @Override
    public void prepare() {
      Builder builder = new AsyncHttpClientConfig.Builder();
      AsyncHttpClientConfig asyncConfig = builder.setAllowPoolingConnection(true)
          .setConnectionTimeoutInMs(HTTP_TIMEOUT)
          .setRequestTimeoutInMs(HTTP_TIMEOUT)
          .setFollowRedirects(false)
          .build();
      _asyncHttpClient = new AsyncHttpClient(asyncConfig);
    }

    @Override
    public int getTargetParallelism() {
      // TODO: fix this
      return 20; 
//        if (config.containsKey("parallelism")) {
//          return Integer.parseInt(  config.get("parallelism").toString() );
//        } else {
//          return super.getTargetParallelism();
//        }
    }

    @Override
    public void execute(List<MapTuple> tuples, final OutputCollector collector) throws OperationException {
      
      if (tuples.isEmpty())
        return;
      
      final CountDownLatch latch = new CountDownLatch(tuples.size());
      
      try { 
        for(final MapTuple t : tuples) {
          
          // Prepare the URL
          String rawUrl = (String)t.get("url");
          if (rawUrl.contains("://") == false) rawUrl = "http://" + rawUrl;
          final URI url = new URI(rawUrl);
          
          // Start the requests...
          _asyncHttpClient.prepareGet(rawUrl).execute(new AsyncCompletionHandler<Void>(){
            
            private int _size = 0;
            
            @Override
            public Void onCompleted(Response response) throws Exception {
              try {
                // Are we dealing with a non-text type? 
                if (response.getContentType() != null && response.getContentType().contains("text") == false) {
                  logger().error("skipping " + url.toString() + " because it is not text");
                  return null;
                }
                
                // Success! 
                MeteredLog.info(logger(), "fetched: " + response.getUri().toString());
                MapTuple t = MapTuple
                    .create("url", url.toString())
                    .put("content", response.getResponseBody())
                    .put("code", response.getStatusCode());
                if (response.isRedirected()) {
                  t.put("redirect", url.resolve(response.getHeader("Location")).toString());
                }
                collector.emit(t);
                return null;
              } catch(Exception e) {
                _log.error("error in fetcher: " + e);
                throw new Exception(e);
              } finally {
                latch.countDown();
              }
            }
 
            @Override
            public void onThrowable(Throwable e){
              latch.countDown();
              logger().error("unable to fetch: " + url.toString() + " (" + e.getMessage() + ")");
              e.printStackTrace();
            }
            
            @Override
            public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
              _size += bodyPart.length();
              if (_size > MAX_BODY_SIZE) {
                logger().error("skipping " + url.toString() + " because it is too large");
                return STATE.ABORT;
              }
              return super.onBodyPartReceived(bodyPart);
            }
            
          });
        }
        
        // Wait for the above to finish...
        latch.await();
        
      } catch (InterruptedException e) {
        _log.error("interrupted");
      } catch (IOException e) {
        throw new OperationException(this, e);
      } catch (URISyntaxException e1) {
        throw new OperationException(this, e1);
      }
      
    }
  }


  protected static final int HTTP_TIMEOUT = 5000;
  private static final int MAX_PARALLELISM = 50;
  private static final int MAX_BODY_SIZE = 400_000;
  private static Logger _log = Utils.getLogger(FetchUrlComponent.class);

  
  public static Component create(final FlowConfig config) {
    
    Component c = new Component("fetch_url", config);
    ComponentStreamBuilder sb = c.createStream(
        new ComponentInput(
            "input",
            ColumnDef.createString("url")
            ), 
        "stream");
    
    sb.aggregate(new Handler("fetch", MAX_PARALLELISM));
    
    sb.outputs(new ComponentOutput(
        "output",
        ColumnDef.createString("url"),
        ColumnDef.createInteger("code"),
        ColumnDef.createString("content")
        ));
    
    
    return c;
  }

}
