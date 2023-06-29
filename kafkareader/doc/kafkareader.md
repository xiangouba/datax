
# KafkaReader 插件文档


___



## 1 快速介绍

KafkaReader插件实现了从Kafka读取数据。


## 2 实现原理

## 3 功能说明

### 3.1 配置样例

* 配置一个从Kafka同步数据到本地的作业:

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
                    "name": "kafkareader",
                    "parameter": {
                        "topic":"TestTopic_BBB1111",
                        "bootstrapServers":"10.1.11.106:9092,10.1.11.107:9092,10.1.11.108:9092",
                        "groupId":"test-consumer-group",
                        "autoOffsetReset":"earliest",
                        "kafkaPartitions":"42",
                        "isList":"0",
                        "column": ["op_type","POLNO"]
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true,
                        "encoding": "UTF-8"
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

	* 描述：kafka分区 <br />

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

