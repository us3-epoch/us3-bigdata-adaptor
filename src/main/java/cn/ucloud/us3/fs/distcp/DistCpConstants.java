package cn.ucloud.us3.fs.distcp;

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 18:42
 */
public class DistCpConstants {
    public static final String CONF_LABEL_WORKSPACE = "us3.distcp.workspace";
    public static final String CONF_LABEL_INPUT_FILE = "us3.distcp.input.file";
    public static final String CONF_LABEL_CHECKSUM = "us3.distcp.checksum";
    public static final String CONF_LABEL_CHECKSUM_ALGORITHM = "us3.distcp.checksum.algorithm";
    public static final String CONF_LABEL_MODTIME = "us3.distcp.modtime";
    public static final String CONF_LABEL_SKIP_CHECK = "us3.distcp.skipcheck";
    public static final String CONF_LABEL_ONLY_CHECK = "us3.distcp.only.check";
    public static final String CONF_LABEL_MAP_NUM = "us3.distcp.mapnum";
    public static final String CONF_LABEL_DUMP = "us3.distcp.dump";
    public static final String CONF_LABEL_ENFORCE_DO = "us3.distcp.enforce";
    public static final String CONF_LABEL_TOTAL_BYTES = "us3.distcp.total.bytes";
    public static final String CONF_LABEL_STAGE = "us3.distcp.stage";
    public static final String STAGE_CHECK = "check";
    public static final String STAGE_CP = "cp";

    public static final String WORKSPACE_DEFAULT = "/us3/distcp/";
    public static final String CHECK_INPUT_DIR = "input";
    public static final String CP_INPUT_DIR = "checkout";
    public static final String CP_OUTPUT_DIR = "cpout";

    public static final String CONF_LABEL_CHECK_INPUT_DIR = "us3.distcp.checkinput";
    public static final String CONF_LABEL_CP_INPUT_DIR = "us3.distcp.cpinput";
    public static final String CONF_LABEL_CP_OUTPUT_DIR = "us3.distcp.cpoutput";
    public static final String INPUT_FILE = "/input";

    public static final int DEFAULT_MAP_NUM = 688;

    public static final String CHECKSUM_UNDO = "undo";
    public static final String CHECKSUM_RANDOME = "randome";
    public static final String CHECKSUM_ALL = "all";

    public static final long CHECKSUM_RANDOME_HEAD_SIZE = 4*1024*1024L;
    public static final long CHECKSUM_RANDOME_TAIL_SIZE = 4*1024*1024L;
    public static final long CHECKSUM_RANDOME_MIDDLE_SIZE = 1024*1024L;
    public static final int CHECKSUM_RANDOME_MIDDLE_COUNT = 4;
    public static final long CHECKSUM_RANDOME_MAX_SIZE = DistCpConstants.CHECKSUM_RANDOME_HEAD_SIZE + DistCpConstants.CHECKSUM_RANDOME_TAIL_SIZE +
            DistCpConstants.CHECKSUM_RANDOME_MIDDLE_SIZE*DistCpConstants.CHECKSUM_RANDOME_MIDDLE_COUNT;

    public static final String CHECKSUM_ALGORITHM_CRC32C = "crc32c";
    public static final String CHECKSUM_ALGORITHM_MD5 = "md5";
    public static final int CHECKSUM_READ_BUFFER_SIZE = 4*1024*1024;

    public static final String DUMP_INPUT = "input";
    public static final String DUMP_CHECK_OUT = "checkout";
    public static final String DUMP_CP_OUT = "cpout";
    public static final String DUMP_NOTHING = "nothing";

    // 全量校验跟拷贝行为一致, 但考虑到计算时延, 比拷贝略少一点, 1G
    public static final long CHECK_CHECKSUM_ALL_MAX_SIZE_PER_MAPTASK = (1L<<30);
    // 随机校验，每个文件最多12M，按照备份1PB数据量，200W文件左右，MAP任务数不得超过10W个，所有每个MAP处理最少60个，每个任务处理最多60*12MB=720MB数据
    public static final long CHECK_CHECKSUM_RANDOME_MAX_SIZE_PER_MAPTASK = 60*12*(1L<<20);
    // 只利用索引进行比较的，MAP执行1min钟，所有每个MAP处理最少3000个
    public static final long CHECK_CHECKSUM_UNDO_MAX_SIZE_PER_MAPTASK = 3000;

    // 因为要保证每个Map任务执行一分钟，减少Map任务频繁启动释放，且写入US3的实测同步速度是在35MB/s，所以5分钟就是10G左右
    // 但实际测试下来发现后端负载增加，4MB分片的时延要到280ms左右，也就是单个文件串行写入速度在15MB/s，所以5分钟就是4.5G左右，按4G算；
    // 那么如果是1PB数据，需要2.6w个map task
    public static final long COPY_MAX_SIZE_PER_MAPTASK = (1L<<32);
}
