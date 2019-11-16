package com.puyong.binlogspringbootdemo.binlog;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BinLogRunner implements CommandLineRunner {

    @Autowired
    private BinLogClient binLogClient;

    @Override
    public void run(String... args) throws Exception {
        log.info("Comming in BinlogRunner ... ");
        binLogClient.connect();
    }
}
