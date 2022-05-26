package flinkcdc.tidb.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCWithSQLReadTidb {

    public static void main(String[] args) throws Exception {

        // 1-获取表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO： 由于增量将数据写入到Hudi表，所以需要启动Flink Checkpoint检查点
        env.setParallelism(1);
        //env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 设置流式模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("CREATE TABLE hs_wide_new (\n" +
                "  id bigint PRIMARY KEY NOT ENFORCED COMMENT 'box_history_new_id',\n" +
                "  cert_name STRING   COMMENT '被采集人姓名',\n" +
                "  cert_no STRING   COMMENT '证件号码',\n" +
                "  cert_type STRING   COMMENT '证件类型',\n" +
                "  mobile_no STRING   COMMENT '手机号',\n" +
                "  box_no STRING  COMMENT '样本号（试剂盒号）',\n" +
                "  box_type STRING  COMMENT '标本类型 ',\n" +
                "  check_num STRING  COMMENT '校验方式 （试剂盒x人） 为检验方式数量',\n" +
                "  specimen_get_time timestamp(3)   COMMENT '标本采集时间',\n" +
                "  create_user_id STRING  COMMENT '采集人员',\n" +
                "  write_type STRING  COMMENT '写入方式 1手动录入 10扫码录入',\n" +
                "  collect_user_id STRING  COMMENT '采集点用户ID',\n" +
                "  collect_id STRING  COMMENT '采样点名称（采集机构）id',\n" +
                "  province STRING  COMMENT '省区划',\n" +
                "  city STRING  COMMENT '市区划',\n" +
                "  area STRING  COMMENT '县/区 区划',\n" +
                "  community STRING  COMMENT '社区',\n" +
                "  street STRING  COMMENT '街道',\n" +
                "  kit_no STRING   COMMENT '箱码',\n" +
                "  specimen_transport_time timestamp(3)  COMMENT '转运时间',\n" +
                "  specimen_receive_time timestamp(3)  COMMENT '标本接收时间',\n" +
                "  org_id STRING   COMMENT '预计接收检测机构id',\n" +
                "  org_user_id STRING   COMMENT '检测人员ID',\n" +
                "  check_result STRING   COMMENT '检测结果',\n" +
                "  test_result_entry_time timestamp(3)  COMMENT '检验结果录入时间',\n" +
                "  province_jc STRING  COMMENT '检测机构省区划',\n" +
                "  city_jc STRING  COMMENT '检测机构市区划',\n" +
                "  area_jc STRING  COMMENT '检测机构县/区 区划',\n" +
                "  community_jc STRING  COMMENT '检测机构社区',\n" +
                "  street_jc STRING  COMMENT '检测机构街道',\n" +
                "  receive_org_id STRING  COMMENT '实际接收检测机构',\n" +
                "  upload_org_id STRING,\n" +
                "  box_history_new_id bigint,\n" +
                "  accept_status int\n" +
                " ) WITH (\n" +
                " 'connector' = 'tidb-cdc',\n" +
                " 'tikv.grpc.timeout_in_ms' = '30000',\n" +
                " 'pd-addresses' = '10.102.6.78:12379',\n" +
                " 'database-name' = 'hs_count',\n" +
                " 'table-name' = 'hs_wide_new',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'tikv.batch_get_concurrency'='2',\n" +
                " 'tikv.grpc.scan_timeout_in_ms'='30000'\n" +
                " )");//
        tableEnv.executeSql("select * from hs_wide_new").print();


    }

}
