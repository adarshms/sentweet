package storm.starter;

import storm.starter.spout.TwitterSampleSpout;
import storm.starter.bolt.ExtractHashTagsBolt;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.bolt.PostRankingsBolt;
import backtype.storm.StormSubmitter;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.generated.AlreadyAliveException;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class SentweetTopology {

    private static final int TOP_N = 10;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;

    public SentweetTopology() throws InterruptedException {
        builder = new TopologyBuilder();
        topologyName = "sentweetTopology";
        topologyConfig = createTopologyConfiguration();
        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
	conf.setNumWorkers(30);
	conf.setMaxSpoutPending(5000);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        builder.setSpout("twitterSpout", new TwitterSampleSpout());
        builder.setBolt("extractHashTags", new ExtractHashTagsBolt(), 4).setNumTasks(4).shuffleGrouping("twitterSpout");
        builder.setBolt("counter", new RollingCountBolt(300, 5), 8).setNumTasks(4).fieldsGrouping("extractHashTags", new Fields("hashtag"));
        builder.setBolt("intermediateRanker", new IntermediateRankingsBolt(TOP_N), 8).setNumTasks(4).fieldsGrouping("counter", new Fields("obj"));
        builder.setBolt("finalRanker", new TotalRankingsBolt(TOP_N, 5)).globalGrouping("intermediateRanker");
        builder.setBolt("rankPoster", new PostRankingsBolt()).shuffleGrouping("finalRanker");
    }

    public void submit() throws Exception {
	StormSubmitter.submitTopology(topologyName, topologyConfig, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        new SentweetTopology().submit();
    }
}
