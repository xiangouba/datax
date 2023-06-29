
# RestFulApiReader 插件文档


___



## 1 快速介绍

RestFulApiReader插件实现了从RestFul风格的API接口读取数据。

**不同于其他关系型数据库，RestFulApiReader不支持FetchSize.**

## 2 实现原理

简而言之，RestFulApiReader通过从RestFul风格的API接口读取数据


## 3 功能说明

### 3.1 配置样例

* 配置一个从RestFulApi同步抽取数据到Mysql的作业:

```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                "reader": {
                    "name": "restfulapireader",
                    "parameter": {
                        "url": "http://127.0.0.1:8080/list",
                        "method": "post",
                        "dataPath": "data",
                        "column": [
                            "id","name","age","create_time"
                        ],
                        "headerInfos": {
                            "aoth": "rh",
                            "token": "123456"
                        },
                        "bodyParam":{
                            "name": "rh",
                            "age": 20
                        },
                        "queryParam":{
                            "id": 1,
                            "name": "rh"
                        }
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "column": [
                            "id",
                            "name",
                            "age",
                            "create_time"
                        ],
                        "connection": [
                            {

                                "jdbcUrl": "jdbc:mysql://localhost:3306/area",
                                "table": ["t"]
                            }
                        ],
                        "password": "******",
                        "username": "root"
                    }
                }
            }
        ]
    }
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

* **column**

	* 描述：字段 不可使用*代替所有字段 例："column": ["id","name","age","create_time",...] <br />

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


### 3.3 类型转换

目前RestFulApiReader支持大部分Mysql类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出RestFulApiReader针对Mysql类型转换列表:


| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext, year   |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。
* `tinyint(1) DataX视作为整形`。
* `year DataX视作为字符串类型`
* `bit DataX属于未定义行为`。

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

**Q: RestFulApiReader同步报错，报错信息为XXX**

 A: maven打包命令：

    mvn -U clean package assembly:assembly -Dmaven.test.skip=true
    
 B: 命令运行
 
    python datax.py ../job/job.json
    
    *：首先maven打包后，在项目的target\datax\datax\bin下运行此命令，job.json文件target\datax\datax\job\job.json
    
 C: channel参数
 
    *** 此reader只"channel":1单通道执行！！！ 无论这里channel为多少，程序目前都只是一次执行

如果上述命令也报错，那可以证实是环境问题，请联系你的DBA。


