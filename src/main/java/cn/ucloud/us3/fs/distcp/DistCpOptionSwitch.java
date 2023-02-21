package cn.ucloud.us3.fs.distcp;

import org.apache.commons.cli.Option;

/**
 * @Name: cn.ucloud.us3.fs.tools
 * @Description: 枚举将配置键映射到distcp命令行选项
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 18:37
 */
public enum  DistCpOptionSwitch {
    WORKSPACE(DistCpConstants.CONF_LABEL_WORKSPACE,
            new Option("workspace", true, "the workspace keeps a directory of state data for the entire verification synchronization phase")),
    MODTIME(DistCpConstants.CONF_LABEL_MODTIME,
            new Option("modtime", false, "check the modification time." +
                    "when the modification time of the source end is later than the destination" +
                    " end, it is considered that it needs to be copied")),
    //CUSTOMIZE_INPUT_FILES(DistCpConstants.CONF_LABEL_INPUT_FILE,
    //        new Option("inputfile", true, "")),
    CHECKSUM(DistCpConstants.CONF_LABEL_CHECKSUM,
            new Option("checksum", true, "to calculate the checksum, 'randome' "+
                    "only compares the head and tail 4MB checksums, and randomly takes 4 1MB fragments " +
                    "in the middle of the file for verification. 'all' will calculate the checksum of the " +
                    "entire file and compare it. not on by default")),
    CHECKSUM_ALGORITHM(DistCpConstants.CONF_LABEL_CHECKSUM_ALGORITHM,
            new Option("algorithm", true, "checksum algorithm, the default is crc32, " +
                    "you can also choose'md5'")),
    SKIP_CHECK(DistCpConstants.CONF_LABEL_SKIP_CHECK,
            new Option("skipcheck", false, "skip all checks. Including file size, modification time, checksum")),
    ONLY_CHECK(DistCpConstants.CONF_LABEL_ONLY_CHECK,
            new Option("onlycheck", false, "Only get the list of files that fail the integrity check, no need to copy")),
    //MAPNUM(DistCpConstants.CONF_LABEL_MAP_NUM,
    //       new Option("mapnum", true, "a number of map task")),
    DUMP(DistCpConstants.CONF_LABEL_DUMP, new Option("dump", true, "Display the corresponding input information, input, checkout, cpout")),
    ENFORCEDO(DistCpConstants.CONF_LABEL_ENFORCE_DO, new Option("enforce", false, "If the status result of the check or copy exists, whether to enforce the check or copy"));

    private final String confLable;
    private final Option option;

    DistCpOptionSwitch(String confLable, Option option) {
        this.confLable = confLable;
        this.option = option;
    }

    public Option getOption() {
        return option;
    }

    public String getSwitch() {
        return option.getOpt();
    }
}
