package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 商品详情
 * @author dingchuangshi
 */
@Data
public class AppNewsDetail {

    /**
     * 页面入口来源：
     *      应用首页=1、
     *      push=2、
     *      详情页相关推荐=3
     */
    private String entry;

    /**
     * 动作：
     *      开始加载=1，
     *      加载成功=2（pv），
     *      加载失败=3,
     *      退出页面=4
     */
    private String action;

    /**
     * 商品ID（服务端下发的ID）
     */
    @JSONField(name = "goods_id")
    private String goodIds;

    /**
     * 商品样式：
     *      0、无图1、
     *      一张大图2、
     *      两张图3、
     *      三张小图4、
     *      一张小图5、
     *      一张大图两张小图
     *      来源于详情页相关推荐的商品，上报样式都为0（因为都是左文右图）
     */
    @JSONField(name = "show_type")
    private String showType;

    /**
     * 页面停留时长：
     *      从商品开始加载时开始计算，
     *      到用户关闭页面所用的时间。
     *      若中途用跳转到其它页面了，
     *      则暂停计时，待回到详情页时恢复计时。
     *      或中途划出的时间超过10分钟，
     *      则本次计时作废，不上报本次数据。如未加载成功退出，则报空。
     */
    @JSONField(name = "new_stay_time")
    private String newsStayTime;

    /**
     * 加载时长：计算页面开始加载到接口返回数据的时间 （开始加载报0，加载成功或加载失败才上报时间）
     */
    @JSONField(name = "loading_time")
    private String loadingTime;

    /**
     * 加载失败码：把加载失败状态码报回来（报空为加载成功，没有失败）
     */
    private String type1;

    /**
     * 分类ID（服务端定义的分类ID）
     */
    private String category;
}
