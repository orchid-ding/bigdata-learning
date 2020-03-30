package bigdata.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义source
 * @author dingchuangshi
 */
public class CustomSource extends AbstractSource implements Configurable, PollableSource {

    private static Logger logger = LoggerFactory.getLogger(CustomSource.class);

    private QueryMysql sqlSourceHelp;

    @Override
    public Status process() throws EventDeliveryException {
        try {
            //查询数据表
            List<List<Object>> result = sqlSourceHelp.executeQuery();
            //存放event的集合
            List<Event> events = new ArrayList<>();
            //存放event头集合
            Map<String, String> header = new HashMap<>();
            //如果有返回数据，则将数据封装为event
            if (!result.isEmpty()) {
                List<String> allRows = sqlSourceHelp.getAllRows(result);
                Event event = null;
                for (String row : allRows) {
                    event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(header);
                    events.add(event);
                }
                //将event写入channel
                this.getChannelProcessor().processEventBatch(events);
                //更新数据表中的offset信息
                sqlSourceHelp.updateOffset2DB(result.size());
            }
            //等待时长
            Thread.sleep(sqlSourceHelp.getRunQueryDelay());
            return Status.READY;
        } catch (InterruptedException e) {
            logger.error("Error procesing row", e);
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        try{
            sqlSourceHelp = new QueryMysql(context);
        }catch (Exception e){
            logger.error(e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping sql source {} ...", getName());
        try {
            //关闭资源
            sqlSourceHelp.close();
        } finally {
            super.stop();
        }
    }
}
