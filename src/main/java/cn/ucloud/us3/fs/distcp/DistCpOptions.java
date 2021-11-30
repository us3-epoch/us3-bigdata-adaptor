package cn.ucloud.us3.fs.distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 14:37
 */

enum CheckSumMode {
    UNDO(DistCpConstants.CHECKSUM_UNDO),
    RANDOME(DistCpConstants.CHECKSUM_RANDOME),
    ALL(DistCpConstants.CHECKSUM_ALL);

    public String flag;
    CheckSumMode(String flag){ this.flag = flag; }
    public String toString() { return this.flag; }
}

enum CheckSumAlogrithm{
    CRC32C(DistCpConstants.CHECKSUM_ALGORITHM_CRC32C),
    MD5(DistCpConstants.CHECKSUM_ALGORITHM_MD5);

    public String flag;
    CheckSumAlogrithm(String flag) { this.flag = flag;}
    public String toString() { return this.flag; }
}

public class DistCpOptions {
    static final Log LOG = LogFactory.getLog(DistCpOptions.class);
    private boolean blocking = true;

    private Path WorkSpace;
    private Path WorkSpaceInput;
    private Path WorkSpaceCheck;
    private Path WorkSpaceCP;

    private Path inputFileListing = null;
    private List<Path> sourcePaths = null;
    private Path targetPath = null;

    private boolean checkModifyTime = false;

    private CheckSumMode checkSumMode = CheckSumMode.UNDO;
    private CheckSumAlogrithm checkSumAlogrithm = CheckSumAlogrithm.CRC32C;

    private int mapNum = DistCpConstants.DEFAULT_MAP_NUM;
    private boolean skipCheck = false;
    private boolean onlyCheck = false;
    private String dump = null;
    private boolean enforced = false;

    DistCpOptions(){
        setWorkSpace(DistCpConstants.WORKSPACE_DEFAULT);
    };

    public void setInputFileListing(Path inputFileListing) {
        this.inputFileListing = inputFileListing;
    }

    public Path getInputFileListing() {
        return this.inputFileListing;
    }

    public void setSourcePaths(List<Path> sourcePaths) {
        this.sourcePaths = sourcePaths;
    }

    public List<Path> getSourcePaths() {
        return sourcePaths;
    }

    public void setTargetPath(Path targetPath) {
        this.targetPath = targetPath;
    }

    public Path getTargetPath() {
        return targetPath;
    }

    public void setWorkSpace(String workSpace) {
        if (workSpace.substring(workSpace.length()-1) != "/") {
            workSpace += "/";
        }
        WorkSpace = new Path(workSpace);
        WorkSpaceInput = new Path(workSpace + DistCpConstants.CHECK_INPUT_DIR);
        WorkSpaceCheck = new Path(workSpace + DistCpConstants.CP_INPUT_DIR);
        WorkSpaceCP = new Path(workSpace + DistCpConstants.CP_OUTPUT_DIR);
    }

    public Path getWorkSpace() { return WorkSpace; }
    public Path getWorkSpaceInput() { return WorkSpaceInput;}
    public Path getWorkSpaceCheck() { return WorkSpaceCheck;}
    public Path getWorkSpaceCP() { return WorkSpaceCP;}

    public void setBlocking(boolean blocking) {
        this.blocking = blocking;
    }

    public boolean isBlocking() { return blocking;}

    public void setCheckModifyTime(boolean checkModifyTime) {
        this.checkModifyTime = checkModifyTime;
    }
    public boolean isCheckModifyTime() { return checkModifyTime; }

    public void setCheckSumMode(CheckSumMode checkSumMode) {
        LOG.info("set check sum mode:" + checkSumMode.toString());
        this.checkSumMode = checkSumMode;
    }
    public CheckSumMode getCheckSumMode() { return checkSumMode; }

    public void setCheckSumAlogrithm(CheckSumAlogrithm checkSumAlogrithm) {
        LOG.info("set check sum alogrithm:" + checkSumAlogrithm.toString());
        this.checkSumAlogrithm = checkSumAlogrithm;
    }
    public CheckSumAlogrithm getCheckSumAlogrithm() { return checkSumAlogrithm; }

    public void setSkipCheck(boolean skipCheck) { this.skipCheck = skipCheck; }
    public boolean isSkipCheck() { return skipCheck; }

    public void setOnlyCheck(boolean onlyCheck) { this.onlyCheck = onlyCheck; }
    public boolean isOnlyCheck() { return onlyCheck; }

    public void setMapNum(int mapNum) { this.mapNum = mapNum; }
    public int getMapNum() { return mapNum; }

    public void setDump(String dump) { this.dump = dump; }
    public String getDump() { return dump; }

    public boolean isEnforced() { return enforced; }
    public void setEnforced(boolean enforced) { this.enforced = enforced; }
}
