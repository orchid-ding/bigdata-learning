package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 消息通知日志
 * @author dingchuangshi
 */
@Data
public class AppNotification {

    /**
     * 动作：
     *      通知产生=1，
     *      通知弹出=2，
     *      通知点击=3，
     *      常驻通知展示（不重复上报，一天之内只报一次）=4
     */
    private String action;

    /**
     * 通知id：
     *      预警通知=1，
     *      天气预报（早=2，晚=3），
     *      常驻=4
     */
    private String type;

    /**
     * 客户端弹出时间
     */
    @JSONField(name = "ap_time")
    private String apTime;

    /**
     * 备用字段
     */
    private String content;
}
