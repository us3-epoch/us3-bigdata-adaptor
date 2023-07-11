package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * US3 contract tests covering deletes.
 */
public class ITestUS3ContractDelete extends AbstractContractDeleteTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }

  @Override
  protected int getTestTimeoutMillis() {
    return 3000 * 1000;
  }
}
