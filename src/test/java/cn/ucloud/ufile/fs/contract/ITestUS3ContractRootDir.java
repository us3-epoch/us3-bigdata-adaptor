package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * root dir operations against an US3 bucket.
 */
public class ITestUS3ContractRootDir extends
    AbstractContractRootDirectoryTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestUS3ContractRootDir.class);

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }

  @Override
  public void testListEmptyRootDirectory() throws IOException {
    for (int attempt = 1, maxAttempts = 10; attempt <= maxAttempts; ++attempt) {
      try {
        super.testListEmptyRootDirectory();
        break;
      } catch (AssertionError | FileNotFoundException e) {
        if (attempt < maxAttempts) {
          LOG.info("Attempt {} of {} for empty root directory test failed.  "
              + "This is likely caused by eventual consistency of S3 "
              + "listings.  Attempting retry.", attempt, maxAttempts);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
            fail("Test interrupted.");
            break;
          }
        } else {
          LOG.error(
              "Empty root directory test failed {} attempts.  Failing test.",
              maxAttempts);
          throw e;
        }
      }
    }
  }
}
