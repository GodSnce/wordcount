package com.lsl.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lishanglai on 2017/11/3.
 */
public class MyCountBolt extends BaseRichBolt {

    OutputCollector collector;
    Map<String,Integer> map = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {

        this.collector = collector;

    }

    public void execute(Tuple tuple) {

        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);

        System.out.println(Thread.currentThread().getId() + "   word:" + word);
        if (map.containsKey(word)){
            Integer count = map.get(word);
            map.put(word,count+num);
        }else {
            map.put(word,num);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        //不输出

    }
}
