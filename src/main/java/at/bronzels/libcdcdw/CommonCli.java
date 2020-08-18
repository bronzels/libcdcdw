package at.bronzels.libcdcdw;

import at.bronzels.libcdcdw.myenum.AppModeEnum;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CommonCli {
    public AppModeEnum appModeEnum = AppModeEnum.SUBMIT;

    public CommandLine getCommandLine(Options options, String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine comm;
        String parseErrPromtp = "解析命令行参数失败";
        try {
            comm = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(parseErrPromtp);
        }
        if (comm == null) {
            throw new IllegalArgumentException(parseErrPromtp);
        }

        return comm;
    }

    public Options buildOptions() {
        Options options = new Options();
        //短选项，长选项，选项后是否有参数，描述
        //帮助
        Option option;

        //destTable
        option = new Option("h", "help", false, "list commandline usage of this app");
        options.addOption(option);

        option = new Option("am", "app mode", true, "yarn local/remote/submit mode for spark/flink etc");
        options.addOption(option);

        return options;
    }

    public boolean parseIsHelp(Options options, String[] args) {
        CommandLine comm = getCommandLine(options, args);

        if (comm.hasOption("am")) {
            String appMode = comm.getOptionValue("am");
            if(appMode != null) {
                appModeEnum = AppModeEnum.fromName(appMode);
            }
        }

        if (comm.hasOption("h")) {
            HelpFormatter hf = new HelpFormatter();
            hf.setWidth(110);
            hf.printHelp("pls input as ", options, true);
            return true;
        }
        else
            return false;
    }

    static public List<String> getNonArgRemoved(List<String> input, String... optionStrs) {
        List<String> ret = new ArrayList<>(input);
        for(String option: optionStrs) {
            if(input.contains(option)) {
                ret.set(input.indexOf(option), null);
            }
        }
        ret = ret.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return ret;
    }

    static public List<String> getArgRemoved(List<String> input, String... optionStrs) {
        List<String> ret = new ArrayList<>(input);
        for(String option: optionStrs) {
            if(input.contains(option)) {
                int indexOp = input.indexOf(option);
                ret.set(indexOp, null);
                ret.set(indexOp+1, null);
            }
        }
        ret = ret.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return ret;
    }


}
