package bigdata.flume;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class CustomInterceptor implements Interceptor {

    /**
     * encrypted_field_index.
     * 指定需要加密的字段下标
     */
    private final String encrypted_field_index;


    /**
     * The out_index.
     * 指定不需要对应列的下标
     */
    private final String out_index;


    /**
     * 提供构建方法，后期可以接受配置文件中的参数
     * @param encrypted_field_index
     * @param out_index
     */
    public CustomInterceptor(String encrypted_field_index, String out_index) {
        this.encrypted_field_index = encrypted_field_index;
        this.out_index = out_index;
    }

    /**
     * 定义拦截器规则
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        if(event == null){
            return null;
        }
       try{
           String line = new String(event.getBody(), Charsets.UTF_8);
           String newLine = "";
           String[] splits = line.split(",");
           for (int i = 0; i < splits.length; i++) {
               // 加密索引
               int encryptedField = Integer.parseInt(encrypted_field_index);
               // 忽略索引
               int outIndex = Integer.parseInt(out_index);

               if(i == encryptedField){
                   // 加密
                   newLine += md5(splits[encryptedField]) + ",";
               }else if(i != outIndex){
                   // 忽略取消数据
                   newLine += splits[i] + ",";
               }
           }
           // 去掉最后一个'，'符号
           newLine = newLine.substring(0,newLine.length() - 1);
           event.setBody(newLine.getBytes(Charsets.UTF_8));
       }catch (Exception e){
           e.printStackTrace();
       }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> out = new ArrayList<Event>();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void close() {

    }

    /**
     * md5加密
     * @return
     */
    public String md5(String plainText){
        byte[] secretBytes = null;
        try {
            MessageDigest instance = MessageDigest.getInstance("md5");
            instance.update(plainText.getBytes());
            secretBytes = instance.digest();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("没有md5这个算法");
            e.printStackTrace();
        }
        String md5Code = new BigInteger(1,secretBytes).toString(16);
        for (int i = 0; i < 32 - md5Code.length(); i++) {
            md5Code ="0" + md5Code;
        }
        return md5Code;
    }


    public static class MyBuilder  implements CustomInterceptor.Builder {
        /**
         * encrypted_field_index.
         * 指定需要加密的字段下标
         */
        private String encrypted_field_index;

        /**
         * The out_index.
         * 指定不需要对应列的下标
         */
        private String out_index;

        @Override
        public CustomInterceptor build() {
            return new CustomInterceptor(encrypted_field_index, out_index);
        }


        @Override
        public void configure(Context context) {
            this.encrypted_field_index = context.getString("encrypted_field_index", "");
            this.out_index = context.getString("out_index", "");
        }
    }
}
