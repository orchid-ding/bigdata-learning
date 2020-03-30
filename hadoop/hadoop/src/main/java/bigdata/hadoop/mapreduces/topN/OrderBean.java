package bigdata.hadoop.mapreduces.topN;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * userid、datetime、title商品标题、unitPrice商品单价、purchaseNum购买量、productId商品ID
 * @author dingchuangshi
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String userId;

    private Date dateTime;

    private String title;

    private double unitPrice;

    private int purchaseNum;

    private String  productId;

    public OrderBean(){}

    public OrderBean(String userId, Date dateTime, String title, double unitPrice, int purchaseNum, String productId) {
        this.userId = userId;
        this.dateTime = dateTime;
        this.title = title;
        this.unitPrice = unitPrice;
        this.purchaseNum = purchaseNum;
        this.productId = productId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(float unitPrice) {
        this.unitPrice = unitPrice;
    }

    public int getPurchaseNum() {
        return purchaseNum;
    }

    public void setPurchaseNum(int purchaseNum) {
        this.purchaseNum = purchaseNum;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "userId='" + userId + '\'' +
                ", dateTime=" + new SimpleDateFormat("yyyy-MM").format(this.getDateTime()) +
                ", title='" + title + '\'' +
                ", unitPrice=" + unitPrice +
                ", purchaseNum=" + purchaseNum +
                ", productId='" + productId + '\'' +
                '}';
    }

    @Override
    public int compareTo(OrderBean other) {
        int compareCode = this.userId.compareTo(other.userId);

        Double thisNum = this.unitPrice * this.purchaseNum;
        Double otherNum = other.getUnitPrice() * other.getPurchaseNum();
        if(compareCode == 0){
            compareCode = new SimpleDateFormat("yyyy-MM").format(this.getDateTime()).compareTo(new SimpleDateFormat("yyyy-MM").format(other.getDateTime()));
            if(compareCode == 0){
                compareCode = otherNum.compareTo(thisNum);
            }
        }

        return compareCode;
    }

    /**
     * userid、datetime、title商品标题、unitPrice商品单价、purchaseNum购买量、productId商品ID
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.userId);
        out.writeLong(this.dateTime.getTime());
        out.writeUTF(this.title);
        out.writeDouble(this.unitPrice);
        out.writeInt(this.purchaseNum);
        out.writeUTF(this.productId);
    }

    /**
     * userid、datetime、title商品标题、unitPrice商品单价、purchaseNum购买量、productId商品ID
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.dateTime = new Date(in.readLong());
        this.title = in.readUTF();
        this.unitPrice = in.readDouble();
        this.purchaseNum = in.readInt();
        this.productId = in.readUTF();

    }
}
