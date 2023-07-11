package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

/**
 * The contract of US3: only enabled if the test bucket is provided.
 */
public class US3Contract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "contract/us3.xml";

  public US3Contract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return "us3";
  }

}
