package bigdata.hadoop.mapreduces.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



/**
 * 分组对象
 * @author dingchuangshi
 */
public class OrderGroupBean extends WritableComparator {

    public OrderGroupBean(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean orderBean = (OrderBean) a;
        OrderBean orderBeanOther = (OrderBean) b;

        int compareCode = orderBean.getUserId().compareTo(orderBeanOther.getUserId());
        if(compareCode == 0){
            compareCode = orderBean.getDateTime().compareTo(orderBeanOther.getDateTime());
        }
        return compareCode;
    }
}