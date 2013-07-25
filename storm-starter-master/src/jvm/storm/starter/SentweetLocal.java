package storm.starter;

import storm.starter.spout.TwitterSampleSpout;
import storm.starter.bolt.ExtractHashTagsBolt;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.bolt.PostRankingsBolt;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class SentweetLocal {

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 5000;
    private static final int TOP_N = 10;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public SentweetLocal() throws InterruptedException {
        builder = new TopologyBuilder();
        topologyName = "sentweetLocal";
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        builder.setSpout("twitterSpout", new TwitterSampleSpout());
        builder.setBolt("extractHashTags", new ExtractHashTagsBolt(), 2).shuffleGrouping("twitterSpout");
        builder.setBolt("counter", new RollingCountBolt(300, 5), 4).fieldsGrouping("extractHashTags", new Fields("hashtag"));
        builder.setBolt("intermediateRanker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("counter", new Fields("obj"));
        builder.setBolt("finalRanker", new TotalRankingsBolt(TOP_N, 5)).globalGrouping("intermediateRanker");
        builder.setBolt("rankPoster", new PostRankingsBolt()).shuffleGrouping("finalRanker");
    }

    public void run() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public static void main(String[] args) throws Exception {
        new SentweetLocal().run();
    }
}
