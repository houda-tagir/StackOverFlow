package com.sightstack.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HBaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.port}")
    private String zookeeperPort;

    @Value("${hbase.table.name}")
    private String tableName;

    @Bean
    public Configuration hbaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        return configuration;
    }

    @Bean
    public Connection hbaseConnection(Configuration configuration) throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }
}