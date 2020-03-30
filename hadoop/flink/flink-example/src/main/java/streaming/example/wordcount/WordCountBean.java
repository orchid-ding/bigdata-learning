package streaming.example.wordcount;

/**
 * @author dingchuangshi
 *
 * 1. 必须有无参的构造函数
 * 2. bykey，sum 名称必须与属性名一致
 */
public class WordCountBean {

    private String word;

    private Integer count;

    public  static WordCountBean of(String word,Integer count){
        return new WordCountBean(word,count);
    }

    public WordCountBean(String word, Integer count) {
        this.word = word;
        this.count = count;
    }
    /**
     * 1. 必须有无参的构造函数
     */
    public WordCountBean() {
    }

    @Override
    public String toString() {
        return "WordCpuntBean{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
