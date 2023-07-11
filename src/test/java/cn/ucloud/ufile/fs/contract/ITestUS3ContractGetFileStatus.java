package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * US3 contract tests covering getFileStatus.
 */
public class ITestUS3ContractGetFileStatus
    extends AbstractContractGetFileStatusTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }

  @Override
  public void teardown() throws Exception {
    getLog().info("FS details {}", getFileSystem());
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
//    S3ATestUtils.disableFilesystemCaching(conf);
//    // aggressively low page size forces tests to go multipage
//    conf.setInt(Constants.MAX_PAGING_KEYS, 2);
    return conf;
  }
}
