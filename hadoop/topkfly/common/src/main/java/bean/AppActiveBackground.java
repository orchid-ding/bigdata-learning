package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * 用户后台活跃
 * @author dingchuangshi
 */
@Data
public class AppActiveBackground {

    /**
     * 1=upgrade,
     * 2=download(下载),
     * 3=plugin_upgrade
     */
    @JSONField(name = "active_source")
    private String activeSource;
}
