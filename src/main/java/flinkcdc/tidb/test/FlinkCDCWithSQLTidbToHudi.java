package flinkcdc.tidb.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class FlinkCDCWithSQLTidbToHudi {

    public static void main(String[] args) throws Exception {

        // 1-获取表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO： 由于增量将数据写入到Hudi表，所以需要启动Flink Checkpoint检查点
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 设置流式模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("CREATE TABLE hs_wide_new (  id bigint PRIMARY KEY NOT ENFORCED COMMENT 'box_history_new_id',  cert_name varchar(50)   COMMENT '被采集人姓名',  cert_no varchar(255)   COMMENT '证件号码',  cert_type varchar(11)   COMMENT '证件类型',  mobile_no varchar(11)   COMMENT '手机号',  box_no varchar(40)  COMMENT '样本号（试剂盒号）',  box_type varchar(11)  COMMENT '标本类型 ',  check_num varchar(4)  COMMENT '校验方式 （试剂盒x人） 为检验方式数量',  specimen_get_time timestamp(3)   COMMENT '标本采集时间',  create_user_id varchar(64)  COMMENT '采集人员',  write_type varchar(11)  COMMENT '写入方式 1手动录入 10扫码录入',  collect_user_id varchar(64)  COMMENT '采集点用户ID',  collect_id varchar(255)  COMMENT '采样点名称（采集机构）id',  province varchar(255)  COMMENT '省区划',  city varchar(255)  COMMENT '市区划',  area varchar(20)  COMMENT '县/区 区划',  community varchar(255)  COMMENT '社区',  street varchar(255)  COMMENT '街道',  kit_no varchar(256)   COMMENT '箱码',  specimen_transport_time timestamp(3)  COMMENT '转运时间',  specimen_receive_time timestamp(3)  COMMENT '标本接收时间',  org_id varchar(255)   COMMENT '预计接收检测机构id',  org_user_id varchar(70)   COMMENT '检测人员ID',  check_result varchar(255)   COMMENT '检测结果',  test_result_entry_time timestamp(3)  COMMENT '检验结果录入时间',  province_jc varchar(255)  COMMENT '检测机构省区划',  city_jc varchar(255)  COMMENT '检测机构市区划',  area_jc varchar(20)  COMMENT '检测机构县/区 区划',  community_jc varchar(255)  COMMENT '检测机构社区',  street_jc varchar(255)  COMMENT '检测机构街道',  receive_org_id varchar(255)  COMMENT '实际接收检测机构',  upload_org_id varchar(255)  COMMENT '实际上传检测结果机构',  box_history_new_id bigint  COMMENT 'box_history_new的id',  accept_status int  COMMENT '1少管 2多管 3废弃 4挂起 20220516新加,之前数据日期不可使用此字段' ) WITH ( 'connector' = 'tidb-cdc', 'tikv.grpc.timeout_in_ms' = '20000', 'pd-addresses' = '10.102.6.78:12379', 'database-name' = 'hs_count', 'table-name' = 'hs_wide_new','scan.startup.mode' = 'latest-offset')");
        tableEnv.executeSql(" CREATE TABLE hudi_hs_wide_new(  id bigint PRIMARY KEY NOT ENFORCED COMMENT 'box_history_new_id',  cert_name STRING   COMMENT '被采集人姓名',  cert_no STRING   COMMENT '证件号码',  cert_type STRING   COMMENT '证件类型',  mobile_no STRING   COMMENT '手机号',  box_no STRING  COMMENT '样本号（试剂盒号）',  box_type STRING  COMMENT '标本类型 ',  check_num STRING  COMMENT '校验方式 （试剂盒x人） 为检验方式数量',  specimen_get_time timestamp(3)   COMMENT '标本采集时间',  create_user_id STRING  COMMENT '采集人员',  write_type STRING  COMMENT '写入方式 1手动录入 10扫码录入',  collect_user_id STRING  COMMENT '采集点用户ID',  collect_id STRING  COMMENT '采样点名称（采集机构）id',  province STRING  COMMENT '省区划',  city STRING  COMMENT '市区划',  area STRING  COMMENT '县/区 区划',  community STRING  COMMENT '社区',  street STRING  COMMENT '街道',  kit_no STRING   COMMENT '箱码',  specimen_transport_time timestamp(3)  COMMENT '转运时间',  specimen_receive_time timestamp(3)  COMMENT '标本接收时间',  org_id STRING   COMMENT '预计接收检测机构id',  org_user_id STRING   COMMENT '检测人员ID',  check_result STRING   COMMENT '检测结果',  test_result_entry_time timestamp(3)  COMMENT '检验结果录入时间',  province_jc STRING  COMMENT '检测机构省区划',  city_jc STRING  COMMENT '检测机构市区划',  area_jc STRING  COMMENT '检测机构县/区 区划',  community_jc STRING  COMMENT '检测机构社区',  street_jc STRING  COMMENT '检测机构街道',  receive_org_id STRING  COMMENT '实际接收检测机构',  upload_org_id STRING  COMMENT '实际上传检测结果机构',  box_history_new_id bigint  COMMENT 'box_history_new的id',  accept_status int  COMMENT '1少管 2多管 3废弃 4挂起 20220516新加,之前数据日期不可使用此字段' )  with(  'connector'='hudi',  'path'= 'hdfs://cdh07:8020/cdc_test/hudi/hs_wide_new'  , 'hoodie.datasource.write.recordkey.field'= 'id'  , 'write.precombine.field'= 'specimen_get_time'  , 'write.tasks'= '1'  , 'compaction.tasks'= '1'  , 'write.rate.limit'= '2000'  , 'table.type'= 'MERGE_ON_READ'   , 'compaction.async.enabled'= 'true'  , 'compaction.trigger.strategy'= 'num_commits'  , 'compaction.delta_commits'= '1'  , 'changelog.enabled'= 'true'  , 'read.streaming.enabled'= 'true'  , 'read.streaming.check-interval'= '3'  , 'hive_sync.enable'= 'true'  , 'hive_sync.mode'= 'hms'  , 'hive_sync.metastore.uris'= 'thrift://cdh06:9083'  , 'hive_sync.jdbc_url'= 'jdbc:hive2://cdh11:10000'  , 'hive_sync.table'= 'hs_wide_new'  , 'hive_sync.db'= 'cdc_test'  , 'hive_sync.username'= 'hdfs'  , 'hive_sync.support_timestamp(3)'= 'true'  )");
        tableEnv.executeSql(
                "insert into hudi_hs_wide_new select id,cert_name,cert_no,cert_type,mobile_no,box_no,box_type,check_num,specimen_get_time ,create_user_id,write_type,collect_user_id,collect_id, province,city,area,community,street,kit_no,specimen_transport_time,specimen_receive_time,org_id,org_user_id,check_result,test_result_entry_time,province_jc,city_jc,area_jc,community_jc,street_jc,receive_org_id,upload_org_id,box_history_new_id,accept_status from hs_wide_new");


    }

}
