package cn.ucloud.us3.fs.distcp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Name: cn.ucloud.us3.fs.distcp
 * @Description: TODO
 * @Author: rick.wu
 * @E-mail: rick.wu@ucloud.cn
 * @Date: 14:16
 */
public class OptionsParser {
    static final Log LOG = LogFactory.getLog(OptionsParser.class);
    private static final Options options = new Options();

    static {
        for (DistCpOptionSwitch option: DistCpOptionSwitch.values()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding option "+ option.getOption());
            }
            options.addOption(option.getOption());
        }
    }

    private static String getVal(CommandLine command, String swtch) {
        String optionValue = command.getOptionValue(swtch);
        if (optionValue == null) {
            return null;
        } else {
            return optionValue.trim();
        }
    }

    private static DistCpOptions parseSourceAndTargetPaths(
            CommandLine command ){
        DistCpOptions option = new DistCpOptions();
        Path targetPath;
        List<Path> sourcePaths = new ArrayList<>();

        //if (command.hasOption(DistCpOptionSwitch.CUSTOMIZE_INPUT_FILES.getSwitch())) {
        //    option.setInputFileListing(new Path(getVal(command, DistCpOptionSwitch.CUSTOMIZE_INPUT_FILES.getSwitch())));
        //}

        if (option.getInputFileListing() == null) {
            // 证明不是自定义列表，需要根据输入源路径和目的路径来生成需要校验的列表
            String[] leftOverArgs = command.getArgs();
            if (leftOverArgs == null || leftOverArgs.length < 1) {
                LOG.info("target path not specified, Should be carried out from the workspace");
                return new DistCpOptions();
            }

            option.setTargetPath(new Path(leftOverArgs[leftOverArgs.length - 1].trim()));

            for (int i=0; i < leftOverArgs.length-1; i++) {
                sourcePaths.add(new Path(leftOverArgs[i].trim()));
            }
            option.setSourcePaths(sourcePaths);
        }
        return option;
    }

    public static DistCpOptions parse(String args[]) throws Exception {

        CommandLineParser parser = new GnuParser();

        CommandLine command;
        try {
            command = parser.parse(options, args, true);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Unable to parse arguments. " +
                    Arrays.toString(args), e);
        }

        DistCpOptions option = parseSourceAndTargetPaths(command);

        //Process all the other option switches and set options appropriately
        if (command.hasOption(DistCpOptionSwitch.WORKSPACE.getSwitch())) {
            option.setWorkSpace(getVal(command, DistCpOptionSwitch.WORKSPACE.getSwitch()));
        }

        if (command.hasOption(DistCpOptionSwitch.MODTIME.getSwitch())) {
            option.setCheckModifyTime(true);
        }

        if (command.hasOption(DistCpOptionSwitch.CHECKSUM.getSwitch())) {
            String checkSum = getVal(command, DistCpOptionSwitch.CHECKSUM.getSwitch());
            switch (checkSum) {
            case DistCpConstants.CHECKSUM_RANDOME:
                option.setCheckSumMode(CheckSumMode.RANDOME);
                break;
            case DistCpConstants.CHECKSUM_ALL:
                option.setCheckSumMode(CheckSumMode.ALL);
                break;
            case DistCpConstants.CHECKSUM_UNDO:
            default:
                throw new IllegalArgumentException("checksum must be 'randome' or 'all'");
            }
        }

        if (command.hasOption(DistCpOptionSwitch.CHECKSUM_ALGORITHM.getSwitch())) {
            String checkSumAlgorithm = getVal(command, DistCpOptionSwitch.CHECKSUM_ALGORITHM.getSwitch());
            switch (checkSumAlgorithm) {
                case DistCpConstants.CHECKSUM_ALGORITHM_CRC32C:
                    option.setCheckSumAlogrithm(CheckSumAlogrithm.CRC32C);
                    break;
                case DistCpConstants.CHECKSUM_ALGORITHM_MD5:
                    option.setCheckSumAlogrithm(CheckSumAlogrithm.MD5);
                    break;
                default:
                    throw new IllegalArgumentException("checksum alogrithm must be 'crc32c' or 'all'");
            }
        }

        if (command.hasOption(DistCpOptionSwitch.SKIP_CHECK.getSwitch())) {
            option.setSkipCheck(true);
        }

        if (command.hasOption(DistCpOptionSwitch.ONLY_CHECK.getSwitch())) {
            option.setOnlyCheck(true);
        }

        //if (command.hasOption(DistCpOptionSwitch.MAPNUM.getSwitch())) {
        //    String mapNumStr = getVal(command, DistCpOptionSwitch.MAPNUM.getSwitch());
        //    LOG.info("map num setting is " + mapNumStr);
        //    option.setMapNum(Integer.parseInt(mapNumStr));
        //}

        if (option.isOnlyCheck() && option.isSkipCheck()) {
            throw new Exception("skipcheck and onlycheck cannot be set at the same time");
        }

        if (command.hasOption(DistCpOptionSwitch.DUMP.getSwitch())) {
            String dump = getVal(command, DistCpOptionSwitch.DUMP.getSwitch());
            switch (dump) {
                case DistCpConstants.DUMP_INPUT:
                    option.setDump(DistCpConstants.DUMP_INPUT);
                    break;
                case DistCpConstants.DUMP_CHECK_OUT:
                    option.setDump(DistCpConstants.DUMP_CHECK_OUT);
                    break;
                case DistCpConstants.DUMP_CP_OUT:
                    option.setDump(DistCpConstants.DUMP_CP_OUT);
                    break;
                default:
                    option.setDump(DistCpConstants.DUMP_NOTHING);
            }
        }

        if (command.hasOption(DistCpOptionSwitch.ENFORCEDO.getSwitch())) {
            option.setEnforced(true);
        }


        return option;
    }

    public static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("distcp OPTIONS [source_path...] <target_path>\n\nOPTIONS", options);
    }
}
