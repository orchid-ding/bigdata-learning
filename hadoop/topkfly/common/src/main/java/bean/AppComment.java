package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 评论
 * @author dingchuangshi
 */
@Data
public class AppComment {

    /**
     * 评论表
     */
    @JSONField(name = "comment_id")
    private int commentId;

    /**
     * 用户id
     */
    @JSONField(name = "user_id")
    private int userId;

    /**
     * 父级评论id(为0则是一级评论,不为0则是回复)
     */
    @JSONField(name = "p_comment_id")
    private  int pCommentId;

    /**
     * 评论内容
     */
    private String content;

    /**
     * 创建时间
     */
    @JSONField(name = "add_time")
    private String addTime;

    /**
     * 评论的相关id
     */
    @JSONField(name = "other_id")
    private int otherId;

    /**
     *  点赞数量
     */
    @JSONField(name = "praise_count")
    private int praiseCount;

    /**
     * 回复数量
     */
    @JSONField(name = "reply_count")
    private int replyCount;
}
