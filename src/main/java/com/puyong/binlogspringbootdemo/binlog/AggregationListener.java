package com.puyong.binlogspringbootdemo.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AggregationListener implements BinaryLogClient.EventListener {

    @Override
    public void onEvent(Event event) {

        EventType eventType = event.getHeader().getEventType();
        log.info("event type: {}", eventType);
        if (eventType == EventType.TABLE_MAP) {
            TableMapEventData data = event.getData();
            log.info("tableName: [{}]", data.getTable());
            log.info("dbName: [{}]", data.getDatabase());
            return;
        }

        if (eventType != EventType.EXT_UPDATE_ROWS
            && eventType != EventType.EXT_WRITE_ROWS
            && eventType != EventType.EXT_DELETE_ROWS) {
            return;
        }

        log.info("eventData [{}]", event.getData().toString());
    }
}
