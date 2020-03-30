package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 点赞
 * @author dingchuangshi
 */
@Data
public class AppPraise {

    /**
     * 主键id
     */
    private int id;

    /**
     * 用户id
     */
    @JSONField(name = "user_id")
    private int userId;

    /**
     * 点赞的对象id
     */
    @JSONField(name = "target_id")
    private int targetId;

    /**
     * 点赞类型
     *      1问答点赞
     *      2问答评论点赞
     *      3 文章点赞数
     *      4 评论点赞
     */
    private int type;

    /**
     * 添加时间
     */
    @JSONField(name = "add_time")
    private String addTime;
}
