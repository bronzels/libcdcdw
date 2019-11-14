package at.bronzels.libcdcdw;

import org.apache.commons.cli.*;

public class CommonCli {
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

    public boolean askHelp(CommandLine comm, Options options) {
        if (!comm.hasOption("h")) {
            HelpFormatter hf = new HelpFormatter();
            hf.setWidth(110);
            hf.printHelp("usage", options, true);
            return true;
        } else return false;
    }


}
