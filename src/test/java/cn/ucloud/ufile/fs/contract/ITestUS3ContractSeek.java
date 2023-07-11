package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * US3 contract tests covering file seek.
 */
public class ITestUS3ContractSeek extends AbstractContractSeekTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }
}
