package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 用户前台活跃
 * @author dingchuangshi
 */
@Data
public class AppActiveForeground {

    /**
     * 推送的消息的id，
     * 如果不是从推送消息打开，传空
     */
    @JSONField(name = "push_id")
    private String pushId;

    /**
     * 1.push 2.icon 3.其他
     */
    private String access;
}
