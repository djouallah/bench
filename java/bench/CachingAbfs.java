package bench;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

/** abfs:// (no TLS suffix) through the same cache: Fabric's table metadata mixes both schemes. */
public class CachingAbfs extends CachingAbfss {
  public CachingAbfs() {
    super(new AzureBlobFileSystem());
  }
}
