package cn.ucloud.ufile.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * US3 contract tests covering rename.
 */
public class ITestUS3ContractRename extends AbstractContractRenameTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new US3Contract(conf);
  }

  @Override
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts the files"
             +" from the source dir into the existing dir"
             +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source-256.txt");
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");

    Path destFilePath = new Path(destDir, "dest-512.txt");
    byte[] destDateset = dataset(512, 'A', 'Z');
    writeDataset(fs, destFilePath, destDateset, destDateset.length, 1024,
        false);
    assertIsFile(destFilePath);

    boolean rename = fs.rename(srcDir, destDir);
    assertFalse("us3 doesn't support rename to non-empty directory", rename);
  }

  @Test
  public void testRenameFileIntoEmptyDir() throws Throwable {
    describe("Verify renaming a file into an existing empty dir puts the files"
            +" from the source dir into the existing dir"
            +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source-256.txt");
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");
    fs.mkdirs(destDir);

    boolean rename = fs.rename(srcFilePath, destDir);

    assertTrue("rename(" + srcFilePath + " into " + destDir + ") returned true", rename);
    Path destFilePath = new Path(destDir, "source-256.txt");
    ContractTestUtils.verifyFileContents(fs, destFilePath, srcDataset);
    ContractTestUtils.assertPathDoesNotExist(fs, "Renamed file", srcFilePath);
  }

  @Test
  public void testSimpleRename() throws Throwable {
    describe("A -> B rename test");
    FileSystem fs = getFileSystem();
    String dir = "simpleRenameTest";
    Path dirPath = path(dir);
    Path srcFilePath = new Path(dirPath, "source-256.txt");
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path dstFilePath = new Path(dirPath, "dest-256.txt");


    boolean rename = fs.rename(srcFilePath, dstFilePath);

    assertTrue("rename(" + srcFilePath + " into " + dstFilePath + ") returned true", rename);
    ContractTestUtils.verifyFileContents(fs, dstFilePath, srcDataset);
    ContractTestUtils.assertPathDoesNotExist(fs, "Renamed file", srcFilePath);
  }

  @Test
  public void testRenameBigFile() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts the files"
            +" from the source dir into the existing dir"
            +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source.txt");
    byte[] srcDataset = dataset(120 * (1 << 20), 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, true);
    Path destDir = path("dest");
    mkdirs(destDir);

    Path destFilePath = new Path(destDir, "dest.txt");
    boolean rename = fs.rename(srcFilePath, destFilePath);
    assertTrue("rename" + srcFilePath + " to " + destFilePath + " failed.", rename);
    ContractTestUtils.verifyFileContents(fs, destFilePath, srcDataset);
    ContractTestUtils.assertPathDoesNotExist(fs, "source file still exist", srcFilePath);
  }
}
