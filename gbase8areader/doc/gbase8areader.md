
# Gbase8aReader 插件文档


___



## 1 快速介绍

Gbase8aReader插件实现了从Gbase读取数据。在底层实现上，Gbase8aReader通过JDBC连接远程Gbase数据库，并执行相应的sql语句将数据从gbase库中SELECT出来。

## 2 实现原理

简而言之，Gbase8aReader通过JDBC连接器连接到远程的Gbase数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程Gbase数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，Gbase8aReader将其拼接为SQL语句发送到Gbase数据库；对于用户配置querySql信息，Gbase8aReader直接将其发送到Gbase数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从Gbase数据库同步抽取数据到Gbase数据库的作业:

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
          "name": "gbase8areader",
          "parameter": {
            "username": "gbase",
            "password": "gbase20110531",
            "column": [
              "age",
              "name"
            ],
            "splitPk": "",
            "connection": [
              {
                "table": [
                  "test_user_source"
                ],
                "jdbcUrl": [
                  "jdbc:gbase://10.1.11.188:15258/test?failoverEnable=true&gclusterId=bmsoft"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "gbase8awriter",
          "parameter": {
            "username": "gbase",
            "password": "gbase20110531",
            "column": [
              "age",
              "name"
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

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，Gbase8aReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，Gbase8aReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照Gbase8a官方规范，并可以填写连接附件控制信息。

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，Gbase8aReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如['\*']。

	  支持列裁剪，即列可以挑选部分列进行导出。

      支持列换序，即列可以不按照表schema信息进行导出。

	  支持常量配置，用户需要按照Gbase8a SQL语法格式:
	  ["id", "\`table\`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
	  id为普通列名，\`table\`为包含保留字的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：Gbase8aReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形数据切分，`不支持浮点、字符串、日期等其他类型`。如果用户指定其他非支持类型，Gbase8aReader将报错！

	  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，DataX视作使用单通道同步该表数据。

	* 必选：否 <br />

	* 默认值：空 <br />

* **where**

	* 描述：筛选条件，Gbase8aReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > $bizdate 。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，DataX均视作同步全量数据。

	* 必选：否 <br />

	* 默认值：无 <br />

* **querySql**

	* 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，DataX系统就会忽略column这些配置型，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

	 `当用户配置querySql时，Gbase8aReader直接忽略column、where条件的配置`，querySql优先级大于column、where选项。querySql和table不能同时存在

	* 必选：否 <br />

	* 默认值：无 <br />


### 3.3 类型转换

目前Gbase8aReader支持大部分Gbase8a类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出Gbase8aReader针对Gbase8a类型转换列表:


| DataX 内部类型| Gbase8a 数据类型    |
| -------- | -----  |
| Long     ||
| Double   ||
| String   ||
| Date     ||
| Boolean  ||
| Bytes    ||



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。
* `tinyint(1) DataX视作为整形`。
* `year DataX视作为字符串类型`
* `bit DataX属于未定义行为`。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征

#### 4.1.2 机器参数

#### 4.1.3 DataX jvm 参数


### 4.2 测试报告

#### 4.2.1 单表测试报告

#### 4.2.2 分表测试报告(2个分库，每个分库16张分表，共计32张分表)

## 5 约束限制

### 5.1 主备同步数据恢复问题

### 5.2 一致性约束

### 5.3 数据库编码问题

### 5.4 增量数据同步

### 5.5 Sql安全性

Gbase8aReader提供querySql语句交给用户自己实现SELECT抽取语句，Gbase8aReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ

***

