package cn.ucloud.us3.fs.distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.util.Times;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @Name: cn.ucloud.us3.fs.tools
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 18:40
 */
public class DistCp extends Configured implements Tool {
    static final Log LOG = LogFactory.getLog(DistCp.class);
    private DistCpOptions inputOptions;
    private boolean clean = false;

    DistCp() {};

    private void createWorkSpace() throws IOException {
        Configuration configuration = getConf();
        inputOptions.getWorkSpace().getFileSystem(configuration).mkdirs(inputOptions.getWorkSpace());
        inputOptions.getWorkSpaceInput().getFileSystem(configuration).mkdirs(inputOptions.getWorkSpaceInput());
    }

    private void deletePath(Path path) throws IOException {
        LOG.info("us3 distcp, drop workspace:" + path);
        path.getFileSystem(getConf()).delete(path, true);
    }

    private void dropWorkSpace() throws IOException {
        deletePath(inputOptions.getWorkSpaceInput());
        deletePath(inputOptions.getWorkSpaceCheck());
        deletePath(inputOptions.getWorkSpaceCP());
    }

    private void envSetting() throws IOException {
        getConf().set(DistCpConstants.CONF_LABEL_CHECK_INPUT_DIR, inputOptions.getWorkSpaceInput().toString());
        getConf().set(DistCpConstants.CONF_LABEL_CP_INPUT_DIR, inputOptions.getWorkSpaceCheck().toString());
        getConf().set(DistCpConstants.CONF_LABEL_CP_OUTPUT_DIR, inputOptions.getWorkSpaceCP().toString());
    }

    private void prepareInput() throws IOException {
        LOG.info("Prepare to enter data, on workspace:" + inputOptions.getWorkSpace());
        FileSystem workspaceFs = inputOptions.getWorkSpace().getFileSystem(getConf());

        workspaceFs.mkdirs(inputOptions.getWorkSpaceInput());
        //workspaceFs.mkdirs(inputOptions.getWorkSpaceCheck());
        //workspaceFs.mkdirs(inputOptions.getWorkSpaceCP());

        if (inputOptions.getInputFileListing() != null) {
            LOG.info("Prepare to enter data by input file:" + inputOptions.getInputFileListing().toString());
            Path input = inputOptions.getInputFileListing();
            if (input.getParent().equals(inputOptions.getWorkSpaceInput())) {
                throw new IOException("input file listing cannot be place under workspace input directory:" +
                        inputOptions.getInputFileListing().toString());
            }

        } else if (inputOptions.getSourcePaths()!= null &&
                !inputOptions.getSourcePaths().isEmpty() &&
                inputOptions.getTargetPath() != null) {
            LOG.info("Prepare to enter data by sources: ");
            for (Path path: inputOptions.getSourcePaths()) {
                LOG.info("- " + path.toString());
            }

            LOG.info("Prepare to enter data by target: " + inputOptions.getTargetPath().toString());
            CopyListing buildListing = new CopyListing(getConf());
            getConf().setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES,
                    buildListing.buildingInputFromSourcesAndTarget(DistCpConstants.STAGE_CHECK, inputOptions.getCheckSumMode().toString(),
                            inputOptions.getSourcePaths(), inputOptions.getTargetPath(),
                            new Path(inputOptions.getWorkSpaceInput()+ DistCpConstants.INPUT_FILE)));;
        }
    }

    private boolean isContinueLastTask() throws IOException {
        try {
            getConf().setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES, CopyListing.countTotalSize(getConf(), DistCpConstants.STAGE_CHECK,
                    inputOptions.getCheckSumMode().toString(), inputOptions.getWorkSpaceInput(), false));
        } catch (FileNotFoundException e) {
            createWorkSpace();
            return false;
        }
        return true;
    }

    public boolean isExist(Path path) throws IOException {
        try {
            FileStatus[] fsts = path.getFileSystem(getConf()).listStatus(path);
            return fsts.length != 0;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    private void doCpJob() throws Exception {
        getConf().set(DistCpConstants.CONF_LABEL_STAGE, DistCpConstants.STAGE_CP);
        Path cp = inputOptions.getWorkSpaceCP();
        cp.getFileSystem(getConf()).delete(cp, true);
        String jobName = "us3 distcp copy";
        String chosenJobName = getConf().get(JobContext.JOB_NAME);
        if (chosenJobName != null) {
            jobName += ":" + chosenJobName;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName(jobName);
        job.setJarByClass(DistCpMapper.class);

        job.setInputFormatClass(DistCpInputFormat.class);
        job.setMapperClass(DistCpMapper.class);

        int mapNum = calculateMapNum(DistCpConstants.STAGE_CP, getConf().getLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES, -1));
        LOG.info("do copy job, num of maps:" + mapNum);
        job.getConfiguration().setInt(JobContext.NUM_MAPS, mapNum);

        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DstValue.class);
        job.setOutputFormatClass(DistCpOutputFormat.class);
        DistCpOutputFormat.setOutputPath(job, inputOptions.getWorkSpaceCP());

        job.getConfiguration().set(org.apache.hadoop.mapreduce.JobContext.MAP_SPECULATIVE, "false");
        job.getConfiguration().setLong(MRJobConfig.TASK_TIMEOUT, 0);

        job.submit();
        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            throw new IOException("us3 distcp copy failed...");
        } else {
            LOG.info("us3 distcp copy taken: " + elapsedTime(Times.elapsed(job.getStartTime(),job.getFinishTime())));
        }
        return;
    }

    private void doCheckJob() throws Exception {
        getConf().setBoolean(DistCpConstants.CONF_LABEL_SKIP_CHECK, inputOptions.isSkipCheck());
        getConf().setBoolean(DistCpConstants.CONF_LABEL_MODTIME, inputOptions.isCheckModifyTime());
        getConf().set(DistCpConstants.CONF_LABEL_CHECKSUM, inputOptions.getCheckSumMode().toString());
        getConf().set(DistCpConstants.CONF_LABEL_CHECKSUM_ALGORITHM, inputOptions.getCheckSumAlogrithm().toString());
        getConf().set(DistCpConstants.CONF_LABEL_STAGE, DistCpConstants.STAGE_CHECK);
        //getConf().setLong(DistCpConstants.CONF_LABEL_CHECKSUM_RANDOM_MAX_SIZE, DistCpConstants.CHECKSUM_RANDOME_MAX_SIZE);

        String jobName="us3 distcp check";
        String chosenJobName = getConf().get(JobContext.JOB_NAME);
        if (chosenJobName != null)
            jobName += ":" +chosenJobName;
        Job job = Job.getInstance(getConf());
        job.setJobName(jobName);
        job.setJarByClass(DistCpMapper.class);

        job.setInputFormatClass(DistCpInputFormat.class);
        job.setMapperClass(DistCpMapper.class);
        int mapNum = calculateMapNum(DistCpConstants.STAGE_CHECK, getConf().getLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES, -1));
        LOG.info("do check job, num of maps:" + mapNum);
        job.getConfiguration().setInt(JobContext.NUM_MAPS, mapNum);

        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DstValue.class);
        job.setOutputFormatClass(DistCpOutputFormat.class);
        DistCpOutputFormat.setOutputPath(job, inputOptions.getWorkSpaceCheck());

        job.getConfiguration().set(org.apache.hadoop.mapreduce.JobContext.MAP_SPECULATIVE, "false");

        job.submit();
        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            throw new IOException("us3 distcp check failed...");
        } else {
            LOG.info("us3 distcp check taken: " + elapsedTime(Times.elapsed(job.getStartTime(),job.getFinishTime())));
        }
        return;
    }

    private String elapsedTime(long elapsed) {
        final long day = TimeUnit.MILLISECONDS.toDays(elapsed);
        final long hours = TimeUnit.MILLISECONDS.toHours(elapsed)
                - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(elapsed));
        final long minutes = TimeUnit.MILLISECONDS.toMinutes(elapsed)
                - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsed));
        final long seconds = TimeUnit.MILLISECONDS.toSeconds(elapsed)
                - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsed));
        final long ms = TimeUnit.MILLISECONDS.toMillis(elapsed)
                - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(elapsed));
        return String.format(String.format("%d Days %d Hours %d Minutes %d Seconds %d Milliseconds", day, hours, minutes, seconds, ms));
    }

    private void dumpPrint(Path input) throws Exception {
        FileStatus[] fss = input.getFileSystem(getConf()).listStatus(input);
        for (FileStatus fts: fss) {
            SequenceFile.Reader reader;
            Text src = new Text();
            DstValue dst = new DstValue();
            try {
                reader = new SequenceFile.Reader(getConf(),
                        SequenceFile.Reader.file(fts.getPath()));
            } catch (EOFException e) {
                LOG.warn(fts.getPath().toString() + " can not read in sequence format, " + e.getMessage());
                continue;
            }

            while (reader.next(src, dst)) {
                System.out.printf("%s|%s|%s\n", src.toString(),
                        dst.getDst().toString(), dst.getErrMsg());
            }
            reader.close();
        }
    }

    private void dump() throws Exception {
        LOG.info("=====================================================================================");
        switch (inputOptions.getDump()) {
            case DistCpConstants.DUMP_INPUT:
                LOG.info("No.1 The verified source and destination information is as follows:");
                dumpPrint(inputOptions.getWorkSpaceInput());
                break;
            case DistCpConstants.DUMP_CHECK_OUT:
                LOG.info("No.2 The source and destination information that failed the verification is as follows:");
                dumpPrint(inputOptions.getWorkSpaceCheck());
                break;
            case DistCpConstants.DUMP_CP_OUT:
                LOG.info("No.3 The source of unsuccessful copy is as follows:");
                dumpPrint(inputOptions.getWorkSpaceCP());
                break;
        }
        LOG.info("=====================================================================================");
    }

    private void mvCpOut2CheckOut() throws IOException {
        Path checkout = inputOptions.getWorkSpaceCheck();
        Path cpout = inputOptions.getWorkSpaceCP();
        FileSystem fs = checkout.getFileSystem(getConf());
        FileStatus[] fss = fs.listStatus(checkout);
        for (FileStatus fsts: fss) {
            fs.delete(fsts.getPath(), true);
        }

        fss = fs.listStatus(cpout);
        for (FileStatus fsts: fss) {
            fs.rename(fsts.getPath(), checkout);
        }
    }

    public void execute() throws Exception {
        if (inputOptions.getDump() != null && inputOptions.getDump() != DistCpConstants.DUMP_NOTHING) {
            // 查看中间状态结果
            dump();
            return;
        }
        envSetting();
        // 先检查已有状态
        if (isContinueLastTask()) {
            LOG.info("=====================================================================================");
            LOG.info("The last task has not been processed yet, on workspace:" + inputOptions.getWorkSpace());
            LOG.info("Start processing the last task......");
            LOG.info("=====================================================================================");
        } else {
            prepareInput();
        }

        if (!inputOptions.isSkipCheck()) {
            long totalSize = getConf().getLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES, -1);
            if (totalSize <= 0) {
                throw new Exception("workspace input: " + inputOptions.getWorkSpaceInput().toString() + " is empty");
            }
            LOG.info("us3 distcp need check size/count is " + totalSize);

            if (isExist(inputOptions.getWorkSpaceCheck())){
                    if (!inputOptions.isEnforced()) {
                        LOG.warn("us3 distcp, The last integrity check result is still in the directory: " + inputOptions.getWorkSpaceCheck().toString()
                                + ", if you need to re-check, you can add the parameter '-enforce'");
                        return;
                    } else {
                        deletePath(inputOptions.getWorkSpaceCheck());
                    }
            }

            doCheckJob();

            if (0 >= CopyListing.countTotalSize(getConf(), DistCpConstants.STAGE_CHECK, inputOptions.getCheckSumMode().toString(),
                    inputOptions.getWorkSpaceCheck(), true)) {
                LOG.info("us3 distcp, all file integrity verification passed, no need to copy ~ ^_^");
                return;
            } else {
                LOG.error("us3 distcp, some files failed the integrity check, need to copy ~ o(╥﹏╥)o");
            }
        } else {
            LOG.info("skip check process");
        }

        if (!inputOptions.isOnlyCheck()) {
            if (isExist(inputOptions.getWorkSpaceCP())) {
                if (!inputOptions.isEnforced()) {
                    LOG.warn("us3 distcp, The last copy result is still in the directory: " + inputOptions.getWorkSpaceCP().toString()
                            + ", if you need to copy again, you can add the parameter '-enforce'");
                    return;
                } else {
                    deletePath(inputOptions.getWorkSpaceCP());
                }
            }

            int tryTimes = 1;
            while (!clean) {
                // 统计需要拷贝的字节数，规划需要启动的Map数量
                long totalSize = CopyListing.countTotalSize(getConf(), DistCpConstants.STAGE_CP,
                        DistCpConstants.CHECKSUM_UNDO, inputOptions.getWorkSpaceCheck(), false);
                if (totalSize <= 0) {
                    LOG.info("us3 distcp, have nothing to copy");
                    return;
                }
                LOG.info("us3 distcp need copy size is " + totalSize);
                getConf().setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES, totalSize);

                doCpJob();
                if (0 >= CopyListing.countTotalSize(getConf(), DistCpConstants.STAGE_CP, DistCpConstants.CHECKSUM_UNDO,
                        inputOptions.getWorkSpaceCP(), true)) {
                    LOG.info("us3 distcp check & copy succ!!");
                    clean = true;
                } else {
                    // 把拷贝失败的文件作为检查的输入
                    if (tryTimes >= 3) {
                        LOG.info("us3 distcp check & copy failure!! Too many retries... stop");
                        break;
                    }
                    LOG.info("us3 distcp check & copy failure!! need try:" + tryTimes);
                    mvCpOut2CheckOut();
                    tryTimes++;
                }
            }
        }
        return;
    }

    @Override
    public int run(String[] argv) throws Exception {
        if (argv.length < 1) {
            OptionsParser.usage();
            return -1;
        }

        try {
            inputOptions = (OptionsParser.parse(argv));
        } catch (Throwable e) {
            LOG.error("Invalid argument: " + e);
            System.err.println("Invalid arguments: " + e.getMessage());
            OptionsParser.usage();
            return -1;
        }

        try {
            execute();
        } catch (Exception e) {
            LOG.error("Exception encountered", e);
            return -2;
        }

        return 0;
    }

    private synchronized void cleanup() {
        if (clean) {
            try {
                dropWorkSpace();
            } catch (IOException e) {
                LOG.error("drop workspace: " + e);
            }
        }
    }

    private static class Cleanup implements Runnable {
        private final DistCp distCp;

        Cleanup(DistCp distCp) {
            this.distCp = distCp;
        }

        @Override
        public void run() {
            distCp.cleanup();
        }
    }

    private int calculateMapNum(String stage, long totalSize) {
        switch (stage) {
        case DistCpConstants.STAGE_CHECK:
            switch (inputOptions.getCheckSumMode().toString()) {
                case DistCpConstants.CHECKSUM_ALL:
                    return (int)(Math.ceil(totalSize*1.0/DistCpConstants.CHECK_CHECKSUM_ALL_MAX_SIZE_PER_MAPTASK*1.0));
                case DistCpConstants.CHECKSUM_RANDOME:
                    return (int)(Math.ceil(totalSize*1.0/DistCpConstants.CHECK_CHECKSUM_RANDOME_MAX_SIZE_PER_MAPTASK*1.0));
                case DistCpConstants.CHECKSUM_UNDO:
                    return (int)(Math.ceil(totalSize*1.0/DistCpConstants.CHECK_CHECKSUM_UNDO_MAX_SIZE_PER_MAPTASK*1.0));
            }
        case DistCpConstants.STAGE_CP:
            LOG.info("calculate map num, total size:" + totalSize + " ,size_per_task:" + DistCpConstants.COPY_MAX_SIZE_PER_MAPTASK);
            LOG.info("calculate map num:" + (int)(Math.ceil(totalSize*1.0/DistCpConstants.COPY_MAX_SIZE_PER_MAPTASK*1.0)));
            return (int)(Math.ceil(totalSize*1.0/DistCpConstants.COPY_MAX_SIZE_PER_MAPTASK*1.0));
        }
        return inputOptions.getMapNum();
    }

    public static void main(String argv[]) {
        int exitCode;
        try {
            DistCp distCp = new DistCp();
            Cleanup clean = new Cleanup(distCp);
            exitCode = ToolRunner.run(new Configuration(), distCp, argv);
        } catch (Exception e) {
            LOG.error("Couldn't complete Distcp operation: ", e);
            exitCode = -2;
        }
        System.exit(exitCode);
    }
}
