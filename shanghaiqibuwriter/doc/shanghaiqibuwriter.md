# DataX Gbase8aWriter


---


## 1 快速介绍

Gbase8aWriter 插件实现了写入数据到 Gbase8a 主库的目的表的功能。在底层实现上， Gbase8aWriter 通过 JDBC 连接远程 Gbase8a 数据库，并执行相应的 insert into ... 或者 ( replace into ...) 的 sql 语句将数据写入 Gbase8a，内部会分批次提交入库，需要数据库本身采用 innodb 引擎。

Gbase8aWriter 面向ETL开发工程师，他们使用 Gbase8aWriter 从数仓导入数据到 Gbase8a。同时 Gbase8aWriter 亦可以作为数据迁移工具为DBA等用户提供服务。


## 2 实现原理

Gbase8aWriter 通过 DataX 框架获取 Reader 生成的协议数据，根据你配置的 `writeMode` 生成


* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

##### 或者

* `replace into...`(没有遇到主键/唯一性索引冲突时，与 insert into 行为一致，冲突时会用新行替换原有行所有字段) 的语句写入数据到 Gbase8a。出于性能考虑，采用了 `PreparedStatement + Batch`，并且设置了：`rewriteBatchedStatements=true`，将数据缓冲到线程上下文 Buffer 中，当 Buffer 累计到预定阈值时，才发起写入请求。

<br />

    注意：目的表所在数据库必须是主库才能写入数据；整个任务至少需要具备 insert/replace into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。


## 3 功能说明

### 3.1 配置样例

* 这里使用一份从内存产生到 Gbase8a 导入的数据。


```
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3,
                "byte": 1048576
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamreader",
                    "parameter": {
                        "column": [
                            {
                                "value": "user_AAA",
                                "type": "string"
                            },
                            {
                                "value": 22,
                                "type": "long"
                            },
                            {
                                "value": 1,
                                "type": "long"
                            },
                            {
                                "value": "1989-06-04 00:00:00",
                                "type": "date"
                            }
                        ],
                        "sliceRecordCount": 1000000
                    }
                },
                "writer": {
                    "name": "gbase8awriter",
                    "parameter": {
                        "username": "gbase",
                        "password": "gbase20110531",
                        "column": [
                            "name",
                            "age",
                            "sex",
                            "birthday"
                        ],
                        "connection": [
                            {
                                "table": [
                                    "test_user_target"
                                ],
                                "jdbcUrl": "jdbc:gbase://10.1.11.188:15258/test?failoverEnable=true&gclusterId=bmsoft"
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

* **jdbcUrl**

	* 描述：目的数据库的 JDBC 连接信息。作业运行时，DataX 会在你提供的 jdbcUrl 后面追加如下属性：yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true

               注意：1、在一个数据库上只能配置一个 jdbcUrl 值。这与 Gbase8aReader 支持多个备库探测不同，因为此处不支持同一个数据库存在多个主库的情况(双主导入数据情况)
                    2、jdbcUrl按照Gbase8a官方规范，并可以填写连接附加控制信息，比如想指定连接编码为 gbk ，则在 jdbcUrl 后面追加属性 useUnicode=true&characterEncoding=gbk。具体请参看 Gbase8a官方文档或者咨询对应 DBA。


 	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：目的数据库的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：目的数据库的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用`*`表示, 例如: `"column": ["*"]`。

			**column配置项必须指定，不能留空！**

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **session**

	* 描述: DataX在获取Gbase8a连接时，执行session指定的SQL语句，修改当前connection session属性

	* 必须: 否

	* 默认值: 空

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from 表名"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **postSql**

	* 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **writeMode**

	* 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 或者 `ON DUPLICATE KEY UPDATE` 语句<br />

	* 必选：是 <br />
	
	* 所有选项：insert/replace/update <br />

	* 默认值：insert <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Gbase8a的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />


### 3.3 类型转换

类似 Gbase8aReader ，目前 Gbase8aWriter 支持大部分 Gbase8a 类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出 Gbase8aWriter 针对 Gbase8a 类型转换列表:


| DataX 内部类型| Gbase8a 数据类型    |
| -------- | -----  |
| Long     ||
| Double   ||
| String   ||
| Date     ||
| Boolean  ||
| Bytes    ||

 * `bit类型目前是未定义类型转换`

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

#### 4.1.2 机器参数

#### 4.1.3 DataX jvm 参数


### 4.2 测试报告

#### 4.2.1 单表测试报告

#### 4.2.2 分表测试报告(2个分库，每个分库4张分表，共计8张分表)

#### 4.2.3 分表测试报告(2个分库，每个分库8张分表，共计16张分表)

#### 4.2.4 性能测试小结


## 5 约束限制


## FAQ

