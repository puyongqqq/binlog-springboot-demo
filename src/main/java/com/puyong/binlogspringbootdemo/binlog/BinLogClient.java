package com.puyong.binlogspringbootdemo.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.puyong.binlogspringbootdemo.binlog.dto.AggregationListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;

@Slf4j
@Component
public class BinLogClient {

    @Value("${adconf.mysql.host}")
    private String host;

    @Value("${adconf.mysql.port}")
    private Integer port;

    @Value("${adconf.mysql.username}")
    private String username;

    @Value("${adconf.mysql.password}")
    private String password;

    @Value("${adconf.mysql.binlogName}")
    private String binlogName;

    @Value("${adconf.mysql.position}")
    private Integer position;

    private BinaryLogClient client;

    @Autowired
    private AggregationListener aggregationListener;

    public void connect() {
        new Thread(() -> {
            client = new BinaryLogClient(host, port, username, password);
            if (!StringUtils.isEmpty(binlogName) && !position.equals(-1)) {
                client.setBinlogFilename(binlogName);
                client.setBinlogPosition(position);
            }
            client.registerEventListener(aggregationListener);

            try {
                log.info("connecting to mysql start");
                client.connect();
                log.info("connecting to mysql done");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                close();
            }
        }).start();
    }


    private void close() {
        try {
            client.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
