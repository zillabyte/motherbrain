package grandmotherbrain.flow.sourcefromrelation;

import java.io.Closeable;
import java.io.IOException;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

/***
 * 
 * @author jake
 *
 */
@NonNullByDefault
public interface ShardReader<T> extends Closeable {
  public @Nullable T nextRecord() throws IOException;
}