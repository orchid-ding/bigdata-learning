package streaming.example.order.join;


/**
 * @author dingchuangshi
 */
public class OrderDetailInfo {

    private String id;

    private String orderTime;

    private String orderProvince;

    public OrderDetailInfo() {
    }

    public OrderDetailInfo(String id, String orderTime, String orderProvince) {
        this.id = id;
        this.orderTime = orderTime;
        this.orderProvince = orderProvince;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public String getOrderProvince() {
        return orderProvince;
    }

    public void setOrderProvince(String orderProvince) {
        this.orderProvince = orderProvince;
    }

    @Override
    public String toString() {
        return "OrderDetailInfo{" +
                "id='" + id + '\'' +
                ", orderTime=" + orderTime +
                ", orderProvince='" + orderProvince + '\'' +
                '}';
    }
}
