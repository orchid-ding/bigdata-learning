package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 错误日志
 * @author dingchuangshi
 */
@Data
public class AppErrorLog {

    /**
     * 错误摘要
     */
    @JSONField(name = "error_brief")
    private String errorBrief;

    /**
     * 错误详情
     */
    @JSONField(name = "error_detail")
    private String errorDetail;
}
