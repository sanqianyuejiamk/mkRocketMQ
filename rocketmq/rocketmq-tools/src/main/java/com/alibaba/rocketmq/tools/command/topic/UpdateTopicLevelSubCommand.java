package com.alibaba.rocketmq.tools.command.topic;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Created by xiafeng
 * on 15-12-6.
 */
public class UpdateTopicLevelSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopicLevel";
    }

    @Override
    public String commandDesc() {
        return "updateTopicLevel from broker and NameServer.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        return null;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {

    }
}
