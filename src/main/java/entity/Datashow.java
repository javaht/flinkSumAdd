package entity;

import lombok.Builder;
import lombok.Data;

import java.sql.Date;

@Data
@Builder
public class Datashow {

    private String database;

    private String windowStart;

    private String windowEnd;

    private Integer count;

    private Date systemTime;
}