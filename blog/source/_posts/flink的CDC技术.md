---
title: flink的CDC技术
date: 2020-12-10 09:25:00
author: nove
categories: 大数据
tags:
  - flink
  - cdc
---
# flink的CDC技术

## 从mysql的binlog谈起

* 记录所有的数据变更和写入
* 用于主从复制和数据恢复
* 必须提交了事务才能记录binlog
* 重启，手动刷新和日志文件大于了指定大小的时候，生成新的日志文件
* 日志格式
  * Statement：记录执行语句，（在主从复制的时候特定的函数会出现特定的问题）
  * Row：记录所有更改前后的内容，（不会出现特定函数的问题，会有大量的日志文件，例如修改操作）
  * Mixed：结核上述两种，按照情况选择要使用的格式

## 优势

* 能够捕获所有数据的变化，捕获完整的变更记录。在异地容灾，数据备份等场景中得到广泛应用，如果是基于查询的 CDC 有可能导致两次查询的中间一部分数据丢失

* 每次 DML 操作均有记录无需像查询 CDC 这样发起全表扫描进行过滤，拥有更高的效率和性能，具有低延迟，不增加数据库负载的优势

* 无需入侵业务，业务解耦，无需更改业务模型

* 捕获删除事件和捕获旧记录的状态，在查询 CDC 中，周期的查询无法感知中间数据是否删除

## kafka

* Debezium构建于kafka上
* source，sink，broker的概念

## Debezium

* 开源的分布式平台

* 绑定数据库后可以对数据库的数据变更做出及时的相应

* 构建于kafka的部署模式&独立部署的模式

* 固定的json响应格式

~~~
{ "schema": {    ...  }, "payload": {    ... }, "schema": {    ... }, "payload": {    ... }, }
~~~
| Item | Field name | Description                                                  |
| :--- | :--------- | :----------------------------------------------------------- |
| 1    | `schema`   | The first `schema` field is part of the event key. It specifies a Kafka Connect schema that describes what is in the event key’s `payload` portion. In other words, the first `schema` field describes the structure of the primary key, or the unique key if the table does not have a primary key, for the table that was changed.  It is possible to override the table’s primary key by setting the [`message.key.columns` connector configuration property](https://debezium.io/documentation/reference/1.3/connectors/mysql.html#mysql-property-message-key-columns). In this case, the first schema field describes the structure of the key identified by that property. |
| 2    | `payload`  | The first `payload` field is part of the event key. It has the structure described by the previous `schema` field and it contains the key for the row that was changed. |
| 3    | `schema`   | The second `schema` field is part of the event value. It specifies the Kafka Connect schema that describes what is in the event value’s `payload` portion. In other words, the second `schema` describes the structure of the row that was changed. Typically, this schema contains nested schemas. |
| 4    | `payload`  | The second `payload` field is part of the event value. It has the structure described by the previous `schema` field and it contains the actual data for the row that was changed. |

## flink

### debezium嵌入flink

* 返回格式

  ~~~json
  Struct{after=Struct{id=2,name=kate,age=28},source=Struct{version=1.2.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=company,table=student,server_id=0,file=mysql-bin.000001,pos=4755,row=0},op=c,ts_ms=1637292711750}
   1，同时存在 beforeStruct 跟 afterStruct数据的话，就代表是update的数据
   2,只存在 beforeStruct 就是delete数据
   3，只存在 afterStruct数据 就是insert数据
  ~~~

 * 转化为json，解析

# flinkSQL

## 语法

```sql
  select TO_TIMESTAMP(tm_v) from tm
  #String 转化为时间类型
```

## 窗口函数

### 滚动窗口

~~~sql
#窗口10s
select userId
, count(*) as orderCount
, max(money) as maxMoney
,min(money) as minMoney
 ,min(createTime) 
,tumble_start(createTime, INTERVAL '10' SECOND) 
,tumble_end(createTime, INTERVAL '10' SECOND) 
from tb_order
 group by userId,tumble(createTime, INTERVAL '10' SECOND) 
~~~

### 滑动窗口

~~~sql
#窗口60s，间隔5s
select userId
, count(*) as orderCount
, max(money) as maxMoney
,min(money) as minMoney
 ,min(createTime) 
,hop_start(createTime, INTERVAL '5' SECOND,INTERVAL '60' SECOND) 
,hop_end(createTime, INTERVAL '5' SECOND,INTERVAL '60' SECOND) 
from tb_order
 group by userId,hop(createTime, INTERVAL '5' SECOND,INTERVAL '60' SECOND) 
~~~

### 累计窗口

~~~sql
#每隔10s统计今天的数据
insert into print_sink
select
 date_format(window_start, 'HH:mm:ss')
 , date_format(window_end, 'HH:mm:ss')
 , count(id)
 , count(distinct id)
  FROM TABLE(
    CUMULATE(TABLE datagen_source, DESCRIPTOR(proc_time), INTERVAL '10' SECOND, INTERVAL '1' DAY))
  GROUP BY window_start, window_end
~~~

## cube函数

### GROUPING SETS

~~~sql
#GROUP SETS 中的表达式可以包含 0 个或多个字段，0 个表示所有行聚合到 1 组。
SELECT window_start, window_end, supplier_id, SUM(price) as price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end, GROUPING SETS ((supplier_id), ());
-- 在 Window 内，按照 supplier_id 分组，和部分组两个规则
  
+------------------+------------------+-------------+-------+
|     window_start |       window_end | supplier_id | price |
+------------------+------------------+-------------+-------+
| 2020-04-15 08:00 | 2020-04-15 08:10 |      (NULL) | 11.00 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier2 |  5.00 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier1 |  6.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |      (NULL) | 10.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier2 |  9.00 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier1 |  1.00 |
+------------------+------------------+-------------+-------+
~~~

### ROLLUP

~~~sql
#ROLLUP(a, b) 等同于 GROUPING SETS ((a), (a, b), ())
SELECT window_start, window_end, supplier_id, SUM(price) as price
FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
GROUP BY window_start, window_end, ROLLUP (supplier_id);
~~~

### CUBE

~~~sql
#CUBE(a, b) 等同于 GROUPING SETS ((a), (b), (a, b), ())
SELECT window_start, window_end, item, supplier_id, SUM(price) as price
  FROM TABLE(
    TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
  GROUP BY window_start, window_end, CUBE (supplier_id, item);
~~~

## demo

https://xie.infoq.cn/article/c664e0a89afb2110db4f18af9

## Window Top-N

只适用流处理模式

计算每 10 分钟内销售额最高的前 3 名供应商。

~~~sql
SELECT *
  FROM (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY window_start, window_end 
            ORDER BY price DESC) as rownum
    FROM (
      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt
      FROM TABLE(
        TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
      GROUP BY window_start, window_end, supplier_id
    )
  ) WHERE rownum <= 3;
  
  
+------------------+------------------+-------------+-------+-----+--------+
|     window_start |       window_end | supplier_id | price | cnt | rownum |
+------------------+------------------+-------------+-------+-----+--------+
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier1 |  6.00 |   2 |      1 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier4 |  5.00 |   1 |      2 |
| 2020-04-15 08:00 | 2020-04-15 08:10 |   supplier2 |  4.00 |   1 |      3 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier5 |  6.00 |   1 |      1 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier2 |  3.00 |   1 |      2 |
| 2020-04-15 08:10 | 2020-04-15 08:20 |   supplier3 |  2.00 |   1 |      3 |
+------------------+------------------+-------------+-------+-----+--------+
~~~





## 表的参数配置

### jdbc

~~~shell
create table dim (
  dim varchar ,
  channel_eight_role_code varchar ,
  channel_source_code varchar,
  CHANNEL_INFO_ID varchar
) with(
  -- 声明连接器类型。flink会通过spi找到连接器，并且进行参数匹配
  'connector.type' = 'jdbc',
  
  -- jdbc的url
  'connector.url' = 'jdbc:mysql://10.25.76.173:3310/ogg_syncer?useUnicode=true&characterEncoding=UTF-8&useSSL=false',
  
  -- 表名称
  'connector.table' = 'epcis_epcisbase_channel_info',
  
  -- 驱动类型
  'connector.driver' = 'com.mysql.jdbc.Driver',
  
  -- 用过名和密码
  'connector.username' = 'root',
  'connector.password' = 'root',

  -- jdbc作为维表的时候，缓存时间。cache默认未开启。
  'connector.lookup.cache.ttl' = '60s',
  
  --  jdbc作为维表的时候，缓存的最大行数。cache默认未开启。
  'connector.lookup.cache.max-rows' = '100000',
  
  -- jdbc作为维表的时候，如果查询失败，最大查询次数
  'connector.lookup.max-retries' = '3',
  
  -- jdbc写入缓存的最大行数。默认值5000
  'connector.write.flush.max-rows' = '5000',
  
  -- jdbc 写入缓存flush时间间隔。默认为0，立即写入
  'connector.write.flush.interval' = '2s',
  
  -- 写入失败，最大重试次数
  'connector.write.max-retries' = '3' 
);

~~~

# flinkCDC进行数据迁移

### 增量读取事实表，关联维度表，间隔+批次写入ck中，完成数据的迁移

#### sql实现数据的读取

~~~java
package cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Date;
import java.util.Properties;

/**
 * @author nove
 * @version 1.0
 * @date 2021/12/10 16:55
 * cdc迁移数据
 */
public class DateTrans {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student{
        private Integer id;
        private String name;
        private Integer age;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public  static class OrderTb   {
        private Integer id;
        private String serialNo;
        private Integer price;
        private Date time;
    }
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("snapshot.mode","schema_only");//debezium增量获取方式配置

        //事实表的增量获取
        SourceFunction<JSONObject> factTb = MySQLSource.<JSONObject>builder()
                .hostname("47.108.204.135")
                .port(3310)
                .databaseList("company") // monitor all tables under inventory database
                .tableList("company.order_tb")//库名+表名字，逗号分隔
                .username("root")
                .password("hDtoLDuI")
                .debeziumProperties(properties)
                .deserializer(new CdcDwdDeserializationSchema()) // converts SourceRecord to String
                .build();

        //维度表的获取
        SourceFunction<JSONObject> dimTb = MySQLSource.<JSONObject>builder()
                .hostname("47.108.204.135")
                .port(3310)
                .databaseList("company") // monitor all tables under inventory database
                .tableList("company.student")//库名+表名字，逗号分隔
                .username("root")
                .password("hDtoLDuI")
                .deserializer(new CdcDwdDeserializationSchema()) // converts SourceRecord to String
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<JSONObject> factSource = env.addSource(factTb);
        DataStreamSource<JSONObject> dimSource = env.addSource(dimTb);


        SingleOutputStreamOperator<OrderTb> factTbDs = factSource.map(new MapFunction<JSONObject, OrderTb>() {
            @Override
            public OrderTb map(JSONObject value) throws Exception {
                return JSON.parseObject(value.toString(), OrderTb.class);
            }
        });
        SingleOutputStreamOperator<Student> dimTbDs = dimSource.map(new MapFunction<JSONObject, Student>() {
            @Override
            public Student map(JSONObject value) throws Exception {
                return JSON.parseObject(value.toString(), Student.class);
            }
        });


        tableEnv.createTemporaryView("fact",factTbDs);
        tableEnv.createTemporaryView("dim",dimTbDs);


        Table table = tableEnv.sqlQuery("select * from fact a join dim b on a.id = b.id");


        table.execute().print();
    }
}


~~~

