
# HndlWriter 插件文档


___



## 1 快速介绍

HndlWriter插件实现了将数据推送至RestFul接口，并且将数据按照所需结构推送到kafka。

**不同于其他关系型数据库，HndlWriter不支持FetchSize.**

## 2 实现原理

简而言之，HndlWriter通过RestFul接口推送数据，并且使用kafkareader推送数据到kafka


## 3 功能说明

### 3.1 配置样例

* 配置一个从kafka同步抽取数据推送到restful接口并写入kafka:

```
{
  "content": [
    {
      "reader": {
        "name": "kafkareader",
        "parameter": {
          "topic": "first",
          "bootstrapServers": "192.168.10.67:9092",
          "kafkaPartitions": "1",
          "isList": 1,
          "groupId": "test-consumer-group",
          "column": [
            "id",
            "name",
            "age"
          ]
        }
      },
      "writer": {
        "name": "hndlwriter",
        "parameter": {
          "topic": "topic",
          "url": "http://454654/test",
          "method": "post",
          "dataPath": "",
          "aesParams": [
            "params"
          ],
          "pushColumns": [
            "name"
          ],
          "columnMap": [
            {
              "columnName": "id",
              "columnType": "Integer"
            },
            {
              "columnName": "name",
              "columnType": "String"
            }
          ],
          "bootstrapServers": ["192.168.10.67:9092"],
          "payFile":"payFileUpload",
          "headerInfos": {},
          "bodyParam": {},
          "queryParam": {}
        }
      }
    }
  ]
}


```


### 3.2 参数说明

* **url**

	* 描述：RestFul风格的API接口 包括http请求 例："http://127.0.0.1:8080/xxx"或者"https://127.0.0.1:8080/xxx"
	
	* 必选：是 <br />

	* 默认值：无 <br />

* **method**

	* 描述：请求类型 post/get 例："method": "post" <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **dataPath**

	* 描述：接口返回的结果集中要同步的数据真实路径 例："dataPath": "xxx" <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **aesParams**

	* 描述：需要加密的湖南接口的参数字段 不可使用*代替所有字段 例："aesParams": ["partnerCode","passTime","insuretype","params",...] <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **pushColumns**

  * 描述：需要推送到接口的数据字段筛选 例："pushColumns": ["name",...] <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **headerInfos**

	*  描述：请求头参数，如可携带key-value键值对的token、aoth。。。 例："headerInfos": {"key": "value"}，多参数用逗号隔开

	* 必选：否 <br />

	* 默认值：无 <br />

* **bodyParam**

	* 描述：body体请求参数

	* 必选：否 <br />

	* 默认值：空 <br />

* **queryParam**

	* 描述：请求参数，key-value键值对格式 例："queryParam":{"id": 1,"name": "xxx"}

	* 必选：否 <br />

	* 默认值：无 <br />
* **topic**

  * 描述：写入kafka的topic

  * 必选：是 <br />

  * 默认值：无 <br />
  
* **columnMap**

     * 描述：写入kafka的数据结构（与读取的字段内容顺序一致）例："columnMap": ["name",...] <br />
  
     * 必选：是 <br />
  
     * 默认值：无 <br />

* **bootstrapServers**

    * 描述：bootstrapServers

	* 必选：是 <br />

	* 默认值：无 <br />

* **payFile**

	* 描述：判断是否是材料数据同步任务（例："payFile":"payFileUpload" ，payFileUpload参数表示是同步材料的任务） <br />
  
    * 必选：否 <br />
  
    * 默认值：无 <br />

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

* 结果集:
    `{
         "msg": "get",
         "code": "200",
         "data": [
             {
                 "id": null,
                 "name": "Aget0",
                 "age": 0,
                 "create_time": null
             }
         ]
    }`

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核 Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz
	2. mem: 48GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* Mysql数据库机器参数为:
	1. cpu: 32核 Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
	2. mem: 256GB
	3. net: 千兆双网卡
	4. disc: BTWL419303E2800RGN  INTEL SSDSC2BB800G4   D2010370

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

## 5 FAQ

***

**Q: HndlWriter同步报错，报错信息为XXX**

 A: maven打包命令：

    mvn -U clean package assembly:assembly -Dmaven.test.skip=true
    
 B: 命令运行
 
    python datax.py ../job/job.json
    
    *：首先maven打包后，在项目的target\datax\datax\bin下运行此命令，job.json文件target\datax\datax\job\job.json

如果上述命令也报错，那可以证实是环境问题，请联系你的DBA。


