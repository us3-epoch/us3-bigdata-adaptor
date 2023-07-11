package cn.ucloud.us3;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class ObjectListRequest {

    private String bucketName;
    private String prefix;
    private String marker;
    private Integer limit;
    private String delimiter;
}
