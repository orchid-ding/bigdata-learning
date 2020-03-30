package streaming.stateful.bean;

/**
 * @author dingchuangshi
 */
public class IndexValue {

    private String index;

    private Long value;

    public IndexValue() {
    }

    private IndexValue(String index, Long value) {
        this.index = index;
        this.value = value;
    }


    public static IndexValue of(String index,Long value){
        return new IndexValue(index,value);
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "IndexValue{" +
                "index='" + index + '\'' +
                ", value=" + value +
                '}';
    }
}
