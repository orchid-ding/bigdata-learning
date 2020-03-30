package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 收藏
 * @author dingchuangshi
 */
@Data
public class AppFavorites {

    /**
     * 主键
     */
    private int id;

    /**
     * 商品id
     */
    @JSONField(name = "course_id")
    private int courseId;

    /**
     * 用户ID
     */
    @JSONField(name = "user_id")
    private int userId;

    /**
     * 创建时间
     */
    @JSONField(name = "add_time")
    private String addTime;
}
