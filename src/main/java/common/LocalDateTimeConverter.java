package common;

import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class LocalDateTimeConverter {

    private static volatile LocalDateTimeConverter instance;
    private final ZoneOffset zoneOffset = ZoneOffset.of("-3");

    private LocalDateTimeConverter() {}

    public static LocalDateTimeConverter getInstance() {
        if (instance == null) {
            synchronized (LocalDateTimeConverter.class) {
                if (instance == null) {
                    instance = new LocalDateTimeConverter();
                }
            }
        }
        return instance;
    }

    public long toTimestamp(LocalDateTime localDateTime) {
        Instant instant = localDateTime.atZone(zoneOffset).toInstant();
        return instant.toEpochMilli();
    }

    public String toTimeFormat(LocalDateTime localDateTime) {
       return localDateTime.format(DateTimeFormatter.BASIC_ISO_DATE);
    }
}
