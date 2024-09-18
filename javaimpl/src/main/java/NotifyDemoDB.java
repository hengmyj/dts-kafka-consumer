import static boot.Boot.boot;
import static recordgenerator.Names.GROUP_NAME;
import static recordgenerator.Names.INITIAL_CHECKPOINT_NAME;
import static recordgenerator.Names.KAFKA_BROKER_URL_NAME;
import static recordgenerator.Names.KAFKA_TOPIC;
import static recordgenerator.Names.PASSWORD_NAME;
import static recordgenerator.Names.SID_NAME;
import static recordgenerator.Names.SUBSCRIBE_MODE_NAME;
import static recordgenerator.Names.USER_NAME;
import static recordgenerator.Names.USE_CONFIG_CHECKPOINT_NAME;

import common.DefaultRecordListener;
import common.RecordListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class NotifyDemoDB {
    private static final Logger log = LoggerFactory.getLogger(NotifyDemoDB.class);

    public static Map<String, RecordListener> buildRecordListener() {
        // user can impl their own listener,value can be mysql, oracle, postgresql
        RecordListener oracleRecordPrintListener = new DefaultRecordListener("mysql");
        return Collections.singletonMap("RecordPrinter", oracleRecordPrintListener);
    }

    /**
     * This demo use  hard coded config. User can modify variable value for test
     * The detailed describe for var in resources/demoConfig
     */
    public static Properties getConfigs() {
        Properties properties = new Properties();
        // user password and sid for auth
        properties.setProperty(USER_NAME, "ardtra");
        properties.setProperty(PASSWORD_NAME, "7a9qMdcccWfdo");
        properties.setProperty(SID_NAME, "dtsn03s8ytrk1po3pa");
        // kafka consumer group general same with sid
        properties.setProperty(GROUP_NAME, "dtsn03s8ytrk1po3pa");
        // topic to consume, partition is 0
        properties.setProperty(KAFKA_TOPIC, "ap_southeast_1_vpc_rm_t4nw35b9p42mcxs8z_dts_tra_r_version2");
        // kafka broker url
        properties.setProperty(KAFKA_BROKER_URL_NAME, "dts-ap-southeast-1.aliyuncs.com:18001");
        // initial checkpoint for first seek(a timestamp to set, eg 1566180200 if you want (Mon Aug 19 10:03:21 CST 2019))
        properties.setProperty(INITIAL_CHECKPOINT_NAME, "1718203484");
        // if force use config checkpoint when start. for checkpoint reset
        properties.setProperty(USE_CONFIG_CHECKPOINT_NAME, "true");
        // use consumer assign or subscribe interface
        // when use subscribe mode, group config is required. kafka consumer group is enabled
        properties.setProperty(SUBSCRIBE_MODE_NAME, "assign");
        return properties;
    }

    public static void main(String[] args) throws InterruptedException {

        try{
            boot(getConfigs(), buildRecordListener());
        }catch(Throwable e){
            log.error("NotifyDemo: failed cause " + e.getMessage(), e);
            throw e;
        } finally {
            System.exit(0);
        }
    }
}
