
# KafkaWriter 插件文档


___



## 1 快速介绍

KafkaWriter插件实现了从Kafka读取数据。


## 2 实现原理

## 3 功能说明

### 3.1 配置样例

* 配置一个从本地同步数据到Kafka的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
            "errorLimit": {
                "record": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                            {
                                "value": "DataX",
                                "type": "string"
                            },
                            {
                                "value": 19890604,
                                "type": "long"
                            },
                            {
                                "value": "1989-06-04 00:00:00",
                                "type": "date"
                            },
                            {
                                "value": true,
                                "type": "bool"
                            },
                            {
                                "value": "test",
                                "type": "bytes"
                            }
                        ],
                        "sliceRecordCount": 1
                    }
                },
                "writer": {
                    "name": "kafkawriter",
                    "parameter": {
                        "topic": "TestTopic_DDD1111",
                        "bootstrapServers": "10.1.11.106:9092,10.1.11.107:9092,10.1.11.108:9092",
                        "column": [
                            {
                                "columnName": "type_string",
                                "columnType": "String"
                            },
                            {
                                "columnName": "type_long",
                                "columnType": "Long"
                            },
                            {
                                "columnName": "type_date",
                                "columnType": "Date"
                            },
                            {
                                "columnName": "type_bool",
                                "columnType": "Boolean"
                            },
                            {
                                "columnName": "type_bytes",
                                "columnType": "Byte"
                            }
                        ]
                    }
                }
            }
        ]
    }
}

```


### 3.2 参数说明

* **topic**

	* 描述：topic <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **bootstrapServers**

	* 描述：bootstrapServers <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **groupId**

	* 描述：groupId <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **autoOffsetReset**

	* 描述：autoOffsetReset，latest 或 earliest <br />

	* 必选：是 <br />

	* 默认值：latest <br />

* **kafkaPartitions**

	* 描述：kafkaPartitions <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **isList**

	* 描述：kafka数据类型，
	    1、列表类型 
	    0、非列表类型 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：字段名

	* 必选：是 <br />

	* 默认值：无 <br />



### 3.3 类型转换

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

#### 4.1.2 机器参数

#### 4.1.3 DataX jvm 参数

### 4.2 测试报告

## 5 约束限制

