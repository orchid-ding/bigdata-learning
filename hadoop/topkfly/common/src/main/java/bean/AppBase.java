package bean;

import lombok.Data;

/**
 * 公共日志
 * @author dingchuangshi
 */
@Data
public class AppBase{

    /**
     *  (String) 设备唯一标识
     */
    private String mid;

    /**
     * (String) 用户uid
     */
    private String uid;

    /**
     * (String) versionCode，程序版本号
     */
    private String vc;

    /**
     * (String) versionName，程序版本名
     */
    private String vn;

    /**
     * (String) 系统语言
     */
    private String l;

    /**
     * (String) 渠道号，应用从哪个渠道来的。
     */
    private String sr;

    /**
     * (String) Android系统版本
     */
    private String os;

    /**
     * (String) 区域
     */
    private String ar;

    /**
     * (String) 手机型号
     */
    private String md;

    /**
     * (String) 手机品牌
     */
    private String ba;

    /**
     * (String) sdkVersion
     */
    private String sv;

    /**
     * (String) gmail
     */
    private String g;

    /**
     * (String) heightXwidth，屏幕宽高
     */
    private String hw;

    /**
     * (String) 客户端日志产生时的时间
     */
    private String t;

    /**
     * (String) 网络模式
     */
    private String nw;

    /**
     * (double) lng经度
     */
    private String ln;

    /**
     * (double) lat 纬度
     */
    private String la;

}
