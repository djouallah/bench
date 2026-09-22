package bench;

import alluxio.hadoop.LocalCacheFileSystem;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem;

/**
 * abfss:// through Alluxio's client-side page cache on local disk.
 *
 * <p>Exists only because {@link LocalCacheFileSystem} has no no-arg constructor: it wraps a
 * FileSystem it is handed and never builds one from configuration, so `fs.abfss.impl` cannot
 * name it directly. This supplies the wrapped ABFS client and initializes it before the cache.
 * Everything else -- open, getFileStatus, listStatus -- is Alluxio's delegation, unchanged.
 */
public class CachingAbfss extends LocalCacheFileSystem {
  private final FileSystem inner;

  public CachingAbfss() {
    this(new SecureAzureBlobFileSystem());
  }

  protected CachingAbfss(FileSystem inner) {
    super(inner);
    this.inner = inner;
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf) throws IOException {
    inner.initialize(uri, conf);
    super.initialize(uri, conf);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      inner.close();
    }
  }
}
