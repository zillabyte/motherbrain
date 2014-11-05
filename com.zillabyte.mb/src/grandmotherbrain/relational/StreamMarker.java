package grandmotherbrain.relational;

import java.io.Serializable;

/***
 * The StreamMarker is used to capture the current position in a stream. This is
 * used so we can 'pick up where we left off' when streaming. 
 * @author jake
 *
 */
public interface StreamMarker extends Serializable {

}
