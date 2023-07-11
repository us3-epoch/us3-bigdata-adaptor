package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * US3 contract tests opening files.
 */
public class ITestUS3ContractOpen extends AbstractContractOpenTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }
}
