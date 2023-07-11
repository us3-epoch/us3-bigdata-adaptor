package org.apache.hadoop.fs.us3;

import cn.ucloud.ufile.fs.contract.US3Contract;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

/**
 * Contract test suite covering US3 integration with DistCp.
 * Uses the block output stream, buffered to disk. This is the
 * recommended output mechanism for DistCP due to its scalability.
 */
public class ITestUS3ContractDistCp extends AbstractContractDistCpTest {

  @Override
  protected int getTestTimeoutMillis() {
    // 涉及大文件上传，超时时长延长到30分钟
    return 30 * 60 * 1000;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }
}
