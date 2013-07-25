package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.HashtagEntity;

public class ExtractHashTagsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 3168353128193457043L;

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Status status = (Status)(tuple.getValue(0));
        HashtagEntity[] hashtags = status.getHashtagEntities();
        if(hashtags != null && hashtags.length > 0)	
        	for(HashtagEntity hashtag : hashtags)
        		collector.emit(new Values(hashtag.getText()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    } 
}