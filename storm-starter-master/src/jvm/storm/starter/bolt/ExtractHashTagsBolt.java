package storm.starter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.HashtagEntity;

public class ExtractHashTagsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3168353128193457043L;
	OutputCollector _collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
	
    public void execute(Tuple tuple) {
        Status status = (Status)(tuple.getValue(0));
        HashtagEntity[] hashtags = status.getHashtagEntities();
        if(hashtags != null && hashtags.length > 0)	
        	for(HashtagEntity hashtag : hashtags)
        		_collector.emit(tuple, new Values(hashtag.getText()));
        	_collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    } 
}
