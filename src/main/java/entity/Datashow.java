package entity;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class Datashow {

    private String databases;

    private String windowStart;

    private String windowEnd;

    private Integer count;

    private String systemTime;
    /*每分钟、每小时、每天*/
    private String  sinkType;
}