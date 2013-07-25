package storm.starter.spout;

import backtype.storm.Config;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterFilterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 7714515098770183148L;
	SpoutOutputCollector _collector;
    	LinkedBlockingQueue<Status> queue = null;
    	TwitterStream _twitterStream;
    
    	@Override
    	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        	queue = new LinkedBlockingQueue<Status>(10000);
        	_collector = collector;
        	StatusListener listener = new StatusListener() {

		    @Override
		    public void onStatus(Status status) {
		        queue.offer(status);
		    }

		    @Override
		    public void onDeletionNotice(StatusDeletionNotice sdn) {
			System.out.println("NOTICE : Got a status deletion notice id:" + sdn.getStatusId());            
		    }

		    @Override
		    public void onTrackLimitationNotice(int i) {
			System.out.println("NOTICE : Got track limitation notice:" + i);
		    }

		    @Override
		    public void onScrubGeo(long l, long l1) {
			System.out.println("NOTICE : Got scrub_geo event userId:" + l + " upToStatusId:" + l1);
		    }

		    @Override
		    public void onException(Exception ex) {
		        ex.printStackTrace();
		    }
        	};
        	TwitterStreamFactory fact = new TwitterStreamFactory(new ConfigurationBuilder().
        		setOAuthAccessToken("187265762-ev3dFoD47ZnX7hzZBLL2LEWuX0XiL5pNadtXFWta").
        		setOAuthAccessTokenSecret("ZhoD6g8WjhBYWx4iF7Z7RngqBzHMYBKt4gsGQmv2pg").
        		setOAuthConsumerKey("MWLr2SaA3vsXNb94rKIpRA").
        		setOAuthConsumerSecret("VkOqipiV0fnzJKCdl7BB6uxSu43MTm02mcyrLkZoUs").build());
		_twitterStream = fact.getInstance();
		_twitterStream.addListener(listener);
		_twitterStream.sample();
    }

    	@Override
    	public void nextTuple() {
		Status ret = queue.poll();
		if(ret==null) {
		    Utils.sleep(100);
		} else {
		    _collector.emit(new Values(ret));
		}
	 }

    	@Override
    	public void close() {
        	_twitterStream.shutdown();
    	}

    	@Override
    	public Map<String, Object> getComponentConfiguration() {
        	Config ret = new Config();
        	ret.setMaxTaskParallelism(1);
        	return ret;
    	}    

    	@Override
    	public void ack(Object id) {
    	}

    	@Override
    	public void fail(Object id) {
    	}

    	@Override
    	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        	declarer.declare(new Fields("tweet"));
    	}
}
