package flinkcdc.tidb.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 基于Flink SQL Connector实现：从Hudi表中加载数据，编写SQL查询
 */
public class FlinkCDCWithSQLReadHudi {

    public static void main(String[] args) {
        // 1-获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2-创建输入表，TODO：加载Hudi表数据
        tableEnv.executeSql(" CREATE TABLE hudi_hs_wide_new(  id bigint PRIMARY KEY NOT ENFORCED COMMENT 'box_history_new_id',  cert_name STRING   COMMENT '被采集人姓名',  cert_no STRING   COMMENT '证件号码',  cert_type STRING   COMMENT '证件类型',  mobile_no STRING   COMMENT '手机号',  box_no STRING  COMMENT '样本号（试剂盒号）',  box_type STRING  COMMENT '标本类型 ',  check_num STRING  COMMENT '校验方式 （试剂盒x人） 为检验方式数量',  specimen_get_time timestamp(3)   COMMENT '标本采集时间',  create_user_id STRING  COMMENT '采集人员',  write_type STRING  COMMENT '写入方式 1手动录入 10扫码录入',  collect_user_id STRING  COMMENT '采集点用户ID',  collect_id STRING  COMMENT '采样点名称（采集机构）id',  province STRING  COMMENT '省区划',  city STRING  COMMENT '市区划',  area STRING  COMMENT '县/区 区划',  community STRING  COMMENT '社区',  street STRING  COMMENT '街道',  kit_no STRING   COMMENT '箱码',  specimen_transport_time timestamp(3)  COMMENT '转运时间',  specimen_receive_time timestamp(3)  COMMENT '标本接收时间',  org_id STRING   COMMENT '预计接收检测机构id',  org_user_id STRING   COMMENT '检测人员ID',  check_result STRING   COMMENT '检测结果',  test_result_entry_time timestamp(3)  COMMENT '检验结果录入时间',  province_jc STRING  COMMENT '检测机构省区划',  city_jc STRING  COMMENT '检测机构市区划',  area_jc STRING  COMMENT '检测机构县/区 区划',  community_jc STRING  COMMENT '检测机构社区',  street_jc STRING  COMMENT '检测机构街道',  receive_org_id STRING  COMMENT '实际接收检测机构',  upload_org_id STRING  COMMENT '实际上传检测结果机构',  box_history_new_id bigint  COMMENT 'box_history_new的id',  accept_status int  COMMENT '1少管 2多管 3废弃 4挂起 20220516新加,之前数据日期不可使用此字段' )  with(  'connector'='hudi',  'path'= 'hdfs://cdh07:8020/cdc_test/hudi/hs_wide_new'  , 'hoodie.datasource.write.recordkey.field'= 'id'  , 'write.precombine.field'= 'specimen_get_time'  , 'write.tasks'= '1'  , 'compaction.tasks'= '1'  , 'write.rate.limit'= '2000'  , 'table.type'= 'MERGE_ON_READ'   , 'compaction.async.enabled'= 'true'  , 'compaction.trigger.strategy'= 'num_commits'  , 'compaction.delta_commits'= '1'  , 'changelog.enabled'= 'true'  , 'read.streaming.enabled'= 'true'  , 'read.streaming.check-interval'= '3'  , 'hive_sync.enable'= 'true'  , 'hive_sync.mode'= 'hms'  , 'hive_sync.metastore.uris'= 'thrift://cdh06:9083'  , 'hive_sync.jdbc_url'= 'jdbc:hive2://cdh11:10000'  , 'hive_sync.table'= 'hs_wide_new'  , 'hive_sync.db'= 'cdc_test'  , 'hive_sync.username'= 'hdfs'  , 'hive_sync.support_timestamp(3)'= 'true'  )");

        // 3-执行查询语句，读取流式读取Hudi表数据
        tableEnv.executeSql(
                "SELECT count(*) FROM hudi_hs_wide_new"
        ).print();
    }

}
