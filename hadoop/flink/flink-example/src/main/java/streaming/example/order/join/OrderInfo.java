package streaming.example.order.join;

/**
 *
 * 订单信息
 * @author dingchuangshi
 */
public class OrderInfo {

    private String id;

    private Double price;

    private String name;

    public OrderInfo() {
    }

    public OrderInfo(String id, Double price, String name) {
        this.id = id;
        this.price = price;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "id='" + id + '\'' +
                ", price=" + price +
                ", name='" + name + '\'' +
                '}';
    }
}
