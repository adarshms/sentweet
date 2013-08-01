package storm.starter.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import net.sf.json.JSONObject;

import storm.starter.tools.Rankable;
import storm.starter.tools.Rankings;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PostRankingsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 8195446835669377072L;
	private static Connection dbConnection = null;
	private PreparedStatement pst = null;

	public Connection getDBConnection() {
	    //String connString = "jdbc:mysql://172.22.138.173:3306/sentweet";
	    String connString = "jdbc:mysql://172.22.138.85:3306/sentweet";
	    String userName = "stuser";
	    String password = "admin123";
		try {
			if(dbConnection == null)
				dbConnection = DriverManager.getConnection(connString, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return dbConnection;
	}
	
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	Rankings rankings = (Rankings)(tuple.getValue(0));
    	List<Rankable> list = rankings.getRankings();
    	//JSONObject json = null;
	String jsonString = "";
		try {
			//json = new JSONObject();
			long unixTime = System.currentTimeMillis() / 1000L;
			jsonString += "{\"created_at\":" + unixTime + ",";
			jsonString += "\"rankings\":[";
			int count = 0;
			for(Rankable obj : list) {
				//json.put(obj.getObject().toString(), obj.getCount());
				if(count > 0)
					jsonString += ",";
				jsonString += "[\"#" + obj.getObject().toString() + "\",[" + obj.getCount() + ",10]]"; 
				count ++;
			}
			jsonString += "]}";
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		try {
	    	Connection conn = getDBConnection();
			if(conn != null) {
				pst = conn.prepareStatement("REPLACE INTO TRENDS (ID, MODULO, RANKLIST) SELECT (COALESCE(MAX(ID), -1) + 1), (COALESCE(MAX(ID), -1) + 1) MOD 60, ? FROM TRENDS");
			    //pst.setString(1, json.toString());
			    pst.setString(1, jsonString);
			    pst.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(pst != null)
					pst.close();
				} catch (SQLException e) {
						e.printStackTrace();
				}
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }   
}
