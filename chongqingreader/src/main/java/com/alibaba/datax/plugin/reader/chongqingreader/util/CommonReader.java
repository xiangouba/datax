package com.alibaba.datax.plugin.reader.chongqingreader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.PreCheckTask;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.reader.chongqingreader.tool.LisBaoquanData;
import com.alibaba.datax.plugin.reader.chongqingreader.tool.LisChengbao;
import com.alibaba.datax.plugin.reader.chongqingreader.tool.LisLipeiData;
import com.alibaba.datax.plugin.reader.chongqingreader.tool.LisLipeiMingxiData;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @Author gxx
 * @Date 2023年06月09日14时53分
 */
public class CommonReader {
    public static class Job {
        private static final Logger logger = LoggerFactory
                .getLogger(CommonRdbmsReader.Job.class);

        public Job(DataBaseType dataBaseType) {
            OriginalConfPretreatmentUtil.DATABASE_TYPE = dataBaseType;
            SingleTableSplitUtil.DATABASE_TYPE = dataBaseType;
        }

        public void init(Configuration originalConfig) {

            OriginalPretreatmentUtil.doPretreatment(originalConfig);

            logger.debug("After job init(), job config now is:[\n{}\n]",
                    originalConfig.toJSON());
        }

        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType) {
            /*检查每个表是否有读权限，以及querySql跟splik Key是否正确*/
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(Key.SPLIT_PK);
            List<Object> connList = queryConf.getList(Constant.CONN_MARK, Object.class);
            String username = queryConf.getString(Key.USERNAME);
            String password = queryConf.getString(Key.PASSWORD);
            ExecutorService exec;
            if (connList.size() < 10) {
                exec = Executors.newFixedThreadPool(connList.size());
            } else {
                exec = Executors.newFixedThreadPool(10);
            }
            Collection<PreCheckTask> taskList = new ArrayList<PreCheckTask>();
            for (int i = 0, len = connList.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connList.get(i).toString());
                PreCheckTask t = new PreCheckTask(username, password, connConf, dataBaseType, splitPK);
                taskList.add(t);
            }
            List<Future<Boolean>> results = Lists.newArrayList();
            try {
                results = exec.invokeAll(taskList);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (Future<Boolean> result : results) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    DataXException de = (DataXException) e.getCause();
                    throw de;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            exec.shutdownNow();
        }


        public List<Configuration> split(Configuration originalConfig,
                                         int adviceNumber) {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber);
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

    }

    public static class Task {
        private static final Logger logger = LoggerFactory
                .getLogger(CommonRdbmsReader.Task.class);
        private static final boolean IS_DEBUG = logger.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private DataBaseType dataBaseType;
        private int taskGroupId = -1;
        private int taskId = -1;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        private String dataFlag;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        public Task(DataBaseType dataBaseType) {
            this(dataBaseType, -1, -1);
        }

        public Task(DataBaseType dataBaseType, int taskGropuId, int taskId) {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGropuId;
            this.taskId = taskId;
        }

        public void init(Configuration readerSliceConfig) {

            /* for database connection */

            this.username = readerSliceConfig.getString(Key.USERNAME);
            this.password = readerSliceConfig.getString(Key.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
            this.dataFlag = readerSliceConfig.getString(Key.DATA_FLAG);

            logger.info("--------------- reader init : " + username + "  " + password + "  " + jdbcUrl + "   " + dataBaseType);

            //ob10的处理
            if (this.jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING) && this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
                }
                logger.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() + ":" + this.username;
                this.jdbcUrl = ss[2];
                logger.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

        }

        public void startRead(Configuration readerSliceConfig,
                              RecordSender recordSender,
                              TaskPluginCollector taskPluginCollector, int fetchSize) {

            String querySql = readerSliceConfig.getString(Key.QUERY_SQL);

            String table = readerSliceConfig.getString(Key.TABLE);

            PerfTrace.getInstance().addTaskDetails(taskId, table + "," + basicMsg);

            logger.info("Begin to read record by Sql: [{}\n] {}.", querySql, basicMsg);
            PerfRecord queryPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
            queryPerfRecord.start();

            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig, this.dataBaseType, basicMsg);
            //在核心数据库查询信息
            String lisName = "";
            String lisPassword = "";
            String lisJdbcUrl = "";
            Connection lisConn = DBUtil.getConnection(this.dataBaseType, lisJdbcUrl,lisName, lisPassword);
            int columnNumber = 0;
            ResultSet rs = null;
            try {
                rs = DBUtil.query(conn, querySql, fetchSize);
                queryPerfRecord.end();

                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                //这个统计干净的result_Next时间
                PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
                allResultPerfRecord.start();

                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber, mandatoryEncoding, taskPluginCollector,lisConn);
                    lastTime = System.nanoTime();
                }

                allResultPerfRecord.end(rsNextUsedTime);
                //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                logger.info("Finished read record by Sql: [{}\n] {}.", querySql, basicMsg);

            } catch (Exception e) {
                throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                            TaskPluginCollector taskPluginCollector,Connection conn) throws SQLException {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding, taskPluginCollector);
            Record recordNew = recordSender.createRecord();
            //Lis核心数据库查询到的保险信息
            ResultSet resultSet = null;
            //TODO 根据column中 id_card 位置确认下标
            String idCard = record.getColumn(0).asString();
            //sql语句
            StringBuilder querySql = new StringBuilder();
            //查询理赔信息、承保信息、保全数据 sql
            switch (this.dataFlag){
                case Constants.LI_BAOQUAN:
                    querySql.append("\n" +
                            "select a.contno as policy_no,\n" +
                            "       (select appntname from lccont where contno = a.contno) as policy_holder,\n" +
                            "       (select appntidno from lccont where contno = a.contno) as policy_holder_id_card,\n" +
                            "       (select (case\n" +
                            "                 when d.RiskType = 'L' then\n" +
                            "                  '寿险'\n" +
                            "                 when d.RiskType = 'A' then\n" +
                            "                  '意外险'\n" +
                            "                 when d.RiskType = 'H' then\n" +
                            "                  '健康险'\n" +
                            "                 else\n" +
                            "                  ''\n" +
                            "               end)\n" +
                            "          from lcpol c, lmriskapp d\n" +
                            "         where c.contno = a.contno\n" +
                            "           and c.riskcode = d.riskcode\n" +
                            "           and c.polno = c.mainpolno\n" +
                            "           and rownum = 1) as insurance_type,\n" +
                            "       (select b.riskname\n" +
                            "          from lmriskapp b, lcpol c\n" +
                            "         where b.riskcode = c.riskcode\n" +
                            "           and c.contno = a.contno\n" +
                            "           and c.polno = a.polno) as insurance_name,\n" +
                            "       (select to_char(to_number(sumprem), '99999999990.99')\n" +
                            "          from lcpol\n" +
                            "         where contno = a.contno\n" +
                            "           and polno = a.polno) as accumulate_premium,\n" +
                            "       to_char(to_number(a.SumMoney), '99999999990.99') as loan_amount,\n" +
                            "       a.LoanDate as loan_time,\n" +
                            "       null as return_amount,\n" +
                            "       null as return_time\n" +
                            "  from loloan a\n" +
                            " where LoanType = '0'\n" +
                            "   and payoffflag = '0'\n" +
                            "      \n" +
                            "   and contno in\n" +
                            "       (select contno\n" +
                            "          from lccont\n" +
                            "         where appntidno = '%s' \n" +
                            "union\n" +
                            "select a.contno as policy_no,\n" +
                            "       (select appntname from lccont where contno = a.contno) as policy_holder,\n" +
                            "       (select appntidno from lccont where contno = a.contno) as policy_holder_id_card,\n" +
                            "       (select (case\n" +
                            "                 when d.RiskType = 'L' then\n" +
                            "                  '寿险'\n" +
                            "                 when d.RiskType = 'A' then\n" +
                            "                  '意外险'\n" +
                            "                 when d.RiskType = 'H' then\n" +
                            "                  '健康险'\n" +
                            "                 else\n" +
                            "                  ''\n" +
                            "               end)\n" +
                            "          from lcpol c, lmriskapp d\n" +
                            "         where c.contno = a.contno\n" +
                            "           and c.riskcode = d.riskcode\n" +
                            "           and c.polno = c.mainpolno\n" +
                            "           and rownum = 1) as insurance_type,\n" +
                            "       (select b.riskname\n" +
                            "          from lmriskapp b, lcpol c\n" +
                            "         where b.riskcode = c.riskcode\n" +
                            "           and c.contno = a.contno\n" +
                            "           and c.polno = a.polno) as insurance_name,\n" +
                            "       (select to_char(to_number(sumprem), '99999999990.99')\n" +
                            "          from lcpol\n" +
                            "         where contno = a.contno\n" +
                            "           and polno = a.polno) as accumulate_premium,\n" +
                            "       to_char(to_number(a.SumMoney), '99999999990.99') as loan_amount,\n" +
                            "       a.LoanDate as loan_time,\n" +
                            "       to_char(to_number(a.ReturnMoney), '99999999990.99') as return_amount,\n" +
                            "       a.NewPayDate as return_time\n" +
                            "  from loreturnloan a\n" +
                            " where LoanType = '0'\n" +
                            "   and makedate > add_months(sysdate, -36)\n" +
                            "      \n" +
                            "   and contno in\n" +
                            "       (select contno\n" +
                            "          from lccont\n" +
                            "         where appntidno = '%s' \n" +
                            "        \n" +
                            "        )\n");
                    break;
                case Constants.LI_CHENBAO:
                    querySql.append("\n" +
                            "select b.contno as policy_no,\n" +
                            "       b.appntname as policy_holder,\n" +
                            "       b.appntidno as policy_holder_id_card,\n" +
                            "       b.insuredname as insured_name,\n" +
                            "       b.insuredidno as Insured_id_card,\n" +
                            "       (select (case\n" +
                            "                 when d.RiskType = 'L' then\n" +
                            "                  '寿险'\n" +
                            "                 when d.RiskType = 'A' then\n" +
                            "                  '意外险'\n" +
                            "                 when d.RiskType = 'H' then\n" +
                            "                  '健康险'\n" +
                            "                 else\n" +
                            "                  ''\n" +
                            "               end)\n" +
                            "          from lcpol c, lmriskapp d\n" +
                            "         where c.contno = b.contno\n" +
                            "           and c.riskcode = d.riskcode\n" +
                            "           and c.polno = c.mainpolno\n" +
                            "           and rownum = 1) as insurance_type, --险种分类 \n" +
                            "       (select d.RiskName\n" +
                            "          from lcpol c, lmriskapp d\n" +
                            "         where c.contno = b.contno\n" +
                            "           and c.riskcode = d.riskcode\n" +
                            "           and c.polno = c.mainpolno\n" +
                            "           and rownum = 1) as insurance_name, --险种名称 \n" +
                            "       (select min(c.EnterAccDate)\n" +
                            "          from LJTempFee c\n" +
                            "         where c.otherno = b.contno) as first_pay_time,\n" +
                            "       (select max(c.EnterAccDate)\n" +
                            "          from LJTempFee c\n" +
                            "         where c.otherno = b.contno) as last_pay_time, --最近缴费日期 \n" +
                            "       (select to_char(round(nvl((sum(SUMPREM)), 0), 2), 'fm999999999990.00')\n" +
                            "          from lcprem\n" +
                            "         where contno = b.contno) as accumulate_premium,\n" +
                            "       (select to_char(round(nvl((sum(c.GetMoney)), 0), 2),\n" +
                            "                       'fm999999999990.00')\n" +
                            "          from ljagetendorse c\n" +
                            "         where c.contno = b.contno\n" +
                            "           and FEEFINATYPE = 'TB'\n" +
                            "           and exists (select 1\n" +
                            "                  from lccont a\n" +
                            "                 where a.contno = c.contno\n" +
                            "                   and a.appflag = '4')) as surrender_value,\n" +
                            "       (case\n" +
                            "         when b.appflag = '1' then\n" +
                            "          '生效'\n" +
                            "         when b.appflag = '4' then\n" +
                            "          '终止'\n" +
                            "         else\n" +
                            "          ''\n" +
                            "       end) as insurance_status, --保险状态 \n" +
                            "       (select d.codename\n" +
                            "          from lcpol c, ldcode d\n" +
                            "         where c.contno = b.contno\n" +
                            "           and d.codetype = 'payintv'\n" +
                            "           and d.code = c.payintv\n" +
                            "           and rownum = 1) as pay_method,\n" +
                            "       (select to_char(round(nvl((sum(e.PayMoney)), 0), 2),\n" +
                            "                       'fm999999999990.00')\n" +
                            "          from LJTempFee e\n" +
                            "         where e.PayDate < sysdate\n" +
                            "           and e.PayDate > (sysdate - 365)\n" +
                            "           and e.otherno = b.contno) as year_premium, --上年缴费总金额 \n" +
                            "       (select to_date(to_char(c.cvalidate, 'yyyy-mm-dd'), 'yyyy-mm-dd')\n" +
                            "          from lcpol c\n" +
                            "         where c.contno = b.contno\n" +
                            "           and c.polno = c.mainpolno\n" +
                            "           and rownum = 1) as start_time\n" +
                            "  from lccont b\n" +
                            " where b.appflag in ('1', '4')\n" +
                            "      \n" +
                            "   and b.appntidno = '%s'");
                    break;
                case Constants.LI_PEI:
                    querySql.append("            \n" +
                            "            select (select max(clmno)\n" +
                            "                      from llclaimdetail d\n" +
                            "                     where d.contno = h.contno) claim_id,\n" +
                            "                   (case (select count(1)\n" +
                            "                        from llbnf\n" +
                            "                       where PayeeIDNo = h.PayeeIDNo)\n" +
                            "                     when 0 then\n" +
                            "                      '0'\n" +
                            "                     else\n" +
                            "                      '1'\n" +
                            "                   end) result_state,\n" +
                            "                   b.contno as policy_no,\n" +
                            "                   b.AppntName as policy_holder,\n" +
                            "                   b.AppntIDNo as policy_holder_id_card,\n" +
                            "                   b.InsuredName as insured_name,\n" +
                            "                   b.InsuredIDNo as insured_id_card,\n" +
                            "                   h.PayeeName as receiver_name,\n" +
                            "                   h.PayeeIDNo as receiver_id_card,\n" +
                            "                   h.bnflinkmode as receiver_phone,\n" +
                            "                   (case (select risktype\n" +
                            "                        from lmriskapp\n" +
                            "                       where subriskflag = 'M'\n" +
                            "                         and riskcode in\n" +
                            "                             (select riskcode\n" +
                            "                                from lcpol\n" +
                            "                               where contno = h.contno)\n" +
                            "                         and ROWNUM = 1)\n" +
                            "                     when 'L' then\n" +
                            "                      '寿险'\n" +
                            "                     when 'A' then\n" +
                            "                      '意外险'\n" +
                            "                     when 'H' then\n" +
                            "                      '健康险'\n" +
                            "                     else\n" +
                            "                      ''\n" +
                            "                   end) insurance_type,\n" +
                            "                   (select riskname\n" +
                            "                      from lmriskapp\n" +
                            "                     where subriskflag = 'M'\n" +
                            "                       and riskcode in\n" +
                            "                           (select riskcode\n" +
                            "                              from lcpol\n" +
                            "                             where contno = h.contno)\n" +
                            "                       and ROWNUM = 1) insurance_name,\n" +
                            "                   nvl((select min(f.EnterAccDate)\n" +
                            "                         from ljagetclaim f, llclaim d, llclaimdetail e\n" +
                            "                        where f.otherno = d.clmno\n" +
                            "                          And d.clmno = e.clmno\n" +
                            "                          And d.givetype = '0'\n" +
                            "                          And e.customerno = h.PayeeNo\n" +
                            "                          And e.contno = h.contno\n" +
                            "                          And d.clmstate = '60'),\n" +
                            "                       (Select min(d.EndCaseDate)\n" +
                            "                          From llclaim d, llclaimdetail e\n" +
                            "                         Where d.clmno = e.clmno\n" +
                            "                           And d.givetype = '0'\n" +
                            "                           And e.customerno = h.PayeeNo\n" +
                            "                           And e.contno = h.contno\n" +
                            "                           And d.clmstate = '60')) first_claim_time,\n" +
                            "                   (Select max(d.EndCaseDate)\n" +
                            "                      From llclaim d, llclaimdetail e\n" +
                            "                     Where d.clmno = e.clmno\n" +
                            "                       And d.givetype = '0'\n" +
                            "                       And e.customerno = h.PayeeNo\n" +
                            "                       And e.contno = h.contno\n" +
                            "                       And d.clmstate = '60') last_claim_time,\n" +
                            "                   (select to_char(round(nvl((Select sum(d.realpay)\n" +
                            "                                               From llclaim       d,\n" +
                            "                                                    llclaimdetail e\n" +
                            "                                              Where d.givetype = '0'\n" +
                            "                                                and e.clmno = d.clmno\n" +
                            "                                                and e.contno = h.contno\n" +
                            "                                                And d.clmstate = '60'\n" +
                            "                                              group by e.contno),\n" +
                            "                                             0),\n" +
                            "                                         2),\n" +
                            "                                   'fm999999999990.00')\n" +
                            "                      from dual) accumulate_premium,\n" +
                            "                   (select to_char(round(nvl(((Select sum(d.realpay)\n" +
                            "                                                 From llclaim       d,\n" +
                            "                                                      llclaimdetail e\n" +
                            "                                                Where d.givetype = '0'\n" +
                            "                                                  And e.clmno = d.clmno\n" +
                            "                                                  and e.contno = h.contno\n" +
                            "                                                  And d.clmstate = '60'\n" +
                            "                                                  and d.EndCaseDate between\n" +
                            "                                                      (select to_date((select to_char(sysdate - 365,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual) and\n" +
                            "                                                      (select to_date((select to_char(sysdate,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual)\n" +
                            "                                                group by e.contno) -\n" +
                            "                                             (select sum(l.pay)\n" +
                            "                                                 from llbalance     l,\n" +
                            "                                                      llclaim       d,\n" +
                            "                                                      llclaimdetail e\n" +
                            "                                                where d.clmno = l.clmno\n" +
                            "                                                  And d.clmno = e.clmno\n" +
                            "                                                  And e.customerno =\n" +
                            "                                                      h.PayeeNo\n" +
                            "                                                  And l.contno = h.contno\n" +
                            "                                                  And l.subfeeoperationtype =\n" +
                            "                                                      'C0101'\n" +
                            "                                                  And l.payflag = '1'\n" +
                            "                                                  And l.getdate between\n" +
                            "                                                      (select to_date((select to_char(sysdate - 365,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual) and\n" +
                            "                                                      (select to_date((select to_char(sysdate,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual))),\n" +
                            "                                             0),\n" +
                            "                                         2),\n" +
                            "                                   'fm999999999990.00')\n" +
                            "                      from dual) year_premium\n" +
                            "              from lccont b, llbnf h\n" +
                            "             where b.contno = h.contno\n" +
                            "               and not exists (Select 'X'\n" +
                            "                      From llclaim d, llclaimdetail e\n" +
                            "                     where d.clmno = e.clmno\n" +
                            "                       And d.realpay = '0'\n" +
                            "                       And d.clmstate <> '60'\n" +
                            "                       And e.customerno = h.PayeeNo\n" +
                            "                       And e.contno = h.contno)\n" +
                            "               and exists\n" +
                            "             (select 1 from llbnf e where  and contno = b.contno)\n" +
                            "            union\n" +
                            "            select (select max(clmno)\n" +
                            "                      from llclaimdetail d\n" +
                            "                     where d.contno = c.contno) claim_id,\n" +
                            "                   (case\n" +
                            "                    (select count(1) from lcinsured where idno = c.idno)\n" +
                            "                     when 0 then\n" +
                            "                      '0'\n" +
                            "                     else\n" +
                            "                      '1'\n" +
                            "                   end) result_state,\n" +
                            "                   b.contno as policy_no,\n" +
                            "                   b.AppntName as policy_holder,\n" +
                            "                   b.AppntIDNo as policy_holder_id_card,\n" +
                            "                   b.InsuredName as insured_name,\n" +
                            "                   b.InsuredIDNo as insured_id_card,\n" +
                            "                   c.name as receiver_name,\n" +
                            "                   c.IDNo as receiver_id_card,\n" +
                            "                   (select bnflinkmode\n" +
                            "                      from llbnf\n" +
                            "                     where clmno = (select max(clmno)\n" +
                            "                                      from llclaimdetail\n" +
                            "                                     where customerno = c.insuredno\n" +
                            "                                       and contno = c.contno)) receiver_phone,\n" +
                            "                   (case (select risktype\n" +
                            "                        from lmriskapp\n" +
                            "                       where subriskflag = 'M'\n" +
                            "                         and riskcode in\n" +
                            "                             (select riskcode\n" +
                            "                                from lcpol\n" +
                            "                               where contno = c.contno)\n" +
                            "                         and ROWNUM = 1)\n" +
                            "                     when 'L' then\n" +
                            "                      '寿险'\n" +
                            "                     when 'A' then\n" +
                            "                      '意外险'\n" +
                            "                     when 'H' then\n" +
                            "                      '健康险'\n" +
                            "                     else\n" +
                            "                      ''\n" +
                            "                   end) insurance_type,\n" +
                            "                   (select riskname\n" +
                            "                      from lmriskapp\n" +
                            "                     where subriskflag = 'M'\n" +
                            "                       and riskcode in\n" +
                            "                           (select riskcode\n" +
                            "                              from lcpol\n" +
                            "                             where contno = c.contno)\n" +
                            "                       and ROWNUM = 1) insurance_name,\n" +
                            "                   nvl((select min(f.EnterAccDate)\n" +
                            "                         from ljagetclaim f, llclaim d, llclaimdetail e\n" +
                            "                        where f.otherno = d.clmno\n" +
                            "                          And d.clmno = e.clmno\n" +
                            "                          And d.givetype = '0'\n" +
                            "                          And e.customerno = c.insuredno\n" +
                            "                          And e.contno = c.contno\n" +
                            "                          And d.clmstate = '60'),\n" +
                            "                       (Select min(d.EndCaseDate)\n" +
                            "                          From llclaim d, llclaimdetail e\n" +
                            "                         Where d.clmno = e.clmno\n" +
                            "                           And d.givetype = '0'\n" +
                            "                           And e.customerno = c.insuredno\n" +
                            "                           And e.contno = c.contno\n" +
                            "                           And d.clmstate = '60')) first_claim_time,\n" +
                            "                   (Select max(d.EndCaseDate)\n" +
                            "                      From llclaim d, llclaimdetail e\n" +
                            "                     Where d.clmno = e.clmno\n" +
                            "                       And d.givetype = '0'\n" +
                            "                       And e.customerno = c.insuredno\n" +
                            "                       And e.contno = c.contno\n" +
                            "                       And d.clmstate = '60') last_claim_time,\n" +
                            "                   (select to_char(round(nvl((Select sum(d.realpay)\n" +
                            "                                               From llclaim       d,\n" +
                            "                                                    llclaimdetail e\n" +
                            "                                              Where d.givetype = '0'\n" +
                            "                                                and e.clmno = d.clmno\n" +
                            "                                                and e.contno = c.contno\n" +
                            "                                                And d.clmstate = '60'\n" +
                            "                                              group by e.contno),\n" +
                            "                                             0),\n" +
                            "                                         2),\n" +
                            "                                   'fm999999999990.00')\n" +
                            "                      from dual) accumulate_premium,\n" +
                            "                   (select to_char(round(nvl(((Select sum(d.realpay)\n" +
                            "                                                 From llclaim       d,\n" +
                            "                                                      llclaimdetail e\n" +
                            "                                                Where d.givetype = '0'\n" +
                            "                                                  and e.clmno = d.clmno\n" +
                            "                                                  and e.contno = c.contno\n" +
                            "                                                  And d.clmstate = '60'\n" +
                            "                                                  And d.EndCaseDate between\n" +
                            "                                                      (select to_date((select to_char(sysdate - 365,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual) and\n" +
                            "                                                      (select to_date((select to_char(sysdate,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual)\n" +
                            "                                                group by e.contno) -\n" +
                            "                                             (select sum(l.pay)\n" +
                            "                                                 from llbalance     l,\n" +
                            "                                                      llclaim       d,\n" +
                            "                                                      llclaimdetail e\n" +
                            "                                                where d.clmno = l.clmno\n" +
                            "                                                  And d.clmno = e.clmno\n" +
                            "                                                  And e.customerno =\n" +
                            "                                                      c.insuredno\n" +
                            "                                                  And l.contno = c.contno\n" +
                            "                                                  And d.clmstate = '60'\n" +
                            "                                                  And l.subfeeoperationtype =\n" +
                            "                                                      'C0101'\n" +
                            "                                                  And l.payflag = '1'\n" +
                            "                                                  And l.getdate between\n" +
                            "                                                      (select to_date((select to_char(sysdate - 365,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual) and\n" +
                            "                                                      (select to_date((select to_char(sysdate,\n" +
                            "                                                                                     'yyyy-mm-dd')\n" +
                            "                                                                        from dual),\n" +
                            "                                                                      'yyyy-mm-dd')\n" +
                            "                                                         from dual))),\n" +
                            "                                             0),\n" +
                            "                                         2),\n" +
                            "                                   'fm999999999990.00')\n" +
                            "                      from dual) year_premium\n" +
                            "              from lccont b, lcinsured c\n" +
                            "             where b.contno = c.contno\n" +
                            "               and not exists (Select 'X'\n" +
                            "                      From llclaim d, llclaimdetail e\n" +
                            "                     where d.clmno = e.clmno\n" +
                            "                       And d.realpay = '0'\n" +
                            "                       And d.clmstate <> '60'\n" +
                            "                       And e.customerno = c.insuredno\n" +
                            "                       And e.contno = c.contno)\n" +
                            "               and exists (select 1\n" +
                            "                      from lcinsured e\n" +
                            "                     where e.idno = '%s'\n" +
                            "                  and contno = c.contno)\n");
                    break;
                case Constants.LI_PEI_MINGXI:
                    querySql.append("            \n" +
                            "            select c.clmno as claim_id,\n" +
                            "                   c.clmno as report_no,\n" +
                            "                   (select to_char(round(nvl(c.realpay, 0), 2),\n" +
                            "                                   'fm999999999990.00')\n" +
                            "                      from dual) premium,\n" +
                            "                   (select ExamDate from LLClaimUWMain where clmno = c.clmno) claim_time,\n" +
                            "                   (case (select AuditConclusion\n" +
                            "                        from LLClaimUWMain\n" +
                            "                       where clmno = c.clmno)\n" +
                            "                     when '0' then\n" +
                            "                      '给付或部分给付'\n" +
                            "                     when '1' then\n" +
                            "                      '全部拒付'\n" +
                            "                     when '2' then\n" +
                            "                      '客户撤案'\n" +
                            "                     when '3' then\n" +
                            "                      '公司撤案'\n" +
                            "                     when '4' then\n" +
                            "                      '通融给付'\n" +
                            "                     when '5' then\n" +
                            "                      '协议给付'\n" +
                            "                     when '6' then\n" +
                            "                      '案件回退'\n" +
                            "                     else\n" +
                            "                      ''\n" +
                            "                   end) claim_reason\n" +
                            "              from llclaim c\n" +
                            "             where exists\n" +
                            "             (select 'X'\n" +
                            "                      from llclaim\n" +
                            "                     where clmno = c.clmno\n" +
                            "                       and EndCaseDate between\n" +
                            "                           (select to_date((select to_char(sysdate - 365,\n" +
                            "                                                          'yyyy-mm-dd')\n" +
                            "                                             from dual),\n" +
                            "                                           'yyyy-mm-dd')\n" +
                            "                              from dual) and (select to_date((select to_char(sysdate,\n" +
                            "                                                                            'yyyy-mm-dd')\n" +
                            "                                                               from dual),\n" +
                            "                                                             'yyyy-mm-dd')\n" +
                            "                                                from dual))\n" +
                            "               and exists\n" +
                            "             (select 1\n" +
                            "                      from llclaimdetail d\n" +
                            "                     where d.clmno = c.clmno\n" +
                            "                       and d.contno in\n" +
                            "                           (select contno from llbnf e where e.payeeidno = '%s'))\n" +
                            "            union\n" +
                            "            select c.clmno as claim_id,\n" +
                            "                   c.clmno as report_no,\n" +
                            "                   (select to_char(round(nvl(c.realpay, 0), 2),\n" +
                            "                                   'fm999999999990.00')\n" +
                            "                      from dual) premium,\n" +
                            "                   (select ExamDate from LLClaimUWMain where clmno = c.clmno) claim_time,\n" +
                            "                   (case (select AuditConclusion\n" +
                            "                        from LLClaimUWMain\n" +
                            "                       where clmno = c.clmno)\n" +
                            "                     when '0' then\n" +
                            "                      '给付或部分给付'\n" +
                            "                     when '1' then\n" +
                            "                      '全部拒付'\n" +
                            "                     when '2' then\n" +
                            "                      '客户撤案'\n" +
                            "                     when '3' then\n" +
                            "                      '公司撤案'\n" +
                            "                     when '4' then\n" +
                            "                      '通融给付'\n" +
                            "                     when '5' then\n" +
                            "                      '协议给付'\n" +
                            "                     when '6' then\n" +
                            "                      '案件回退'\n" +
                            "                     else\n" +
                            "                      ''\n" +
                            "                   end) claim_reason\n" +
                            "              from llclaim c\n" +
                            "             where exists\n" +
                            "             (select 'X'\n" +
                            "                      from llclaim\n" +
                            "                     where clmno = c.clmno\n" +
                            "                       and EndCaseDate between\n" +
                            "                           (select to_date((select to_char(sysdate - 365,\n" +
                            "                                                          'yyyy-mm-dd')\n" +
                            "                                             from dual),\n" +
                            "                                           'yyyy-mm-dd')\n" +
                            "                              from dual) and (select to_date((select to_char(sysdate,\n" +
                            "                                                                            'yyyy-mm-dd')\n" +
                            "                                                               from dual),\n" +
                            "                                                             'yyyy-mm-dd')\n" +
                            "                                                from dual))\n" +
                            "               and exists\n" +
                            "             (select 1\n" +
                            "                      from llclaimdetail d\n" +
                            "                     where d.clmno = c.clmno\n" +
                            "                       and d.contno in\n" +
                            "                           (select contno from lcinsured e where e.idno = '%s'))\n");
                    break;
                default:
                    throw new RuntimeException("json参数中配置的 dataFlag 参数内容错误，请根据模板参数参照设定该参数！");

            }
            //sql 参数
            String sql = String.format(querySql.toString(),idCard);
            try {
                resultSet = DBUtil.query(conn, sql,1000);
            } catch (SQLException e) {
                logger.info("{}查询核心数据库的sql语句：{}",this.dataFlag,sql);
                throw new RuntimeException("核心的数据库Lis查询出错",e);
            }
            switch (this.dataFlag){
                case Constants.LI_CHENBAO:
                    dataHandleChengBao(resultSet,record,recordNew);
                    break;
                case Constants.LI_BAOQUAN:
                    dataHandleBaoQuan(resultSet,record,recordNew);
                    break;
                case Constants.LI_PEI:
                    dataHandleLiPei(resultSet,record,recordNew);
                    break;
                case Constants.LI_PEI_MINGXI:
                    dataHandleLiPeiMX(resultSet,record,recordNew);
                    break;
                default:
                    break;
            }
            recordSender.sendToWriter(recordNew);
            return recordNew;
        }

        /*****************************************************************************************/
        /*************************************** 理赔数据处理 ***************************************/
        protected void dataHandleLiPeiMX(ResultSet resultSet,Record record,Record recordNew) throws SQLException {
            List<Map<String,Object>> mapList = new ArrayList<>();
                ResultSetMetaData meta = resultSet.getMetaData();
                int count = meta.getColumnCount();
                //将查询结果放入 mapList
                while (resultSet.next()){
                    Map mapOfColumnValues = new HashMap(count);
                    for (int i = 1; i <= count; i++) {
                        mapOfColumnValues.put(meta.getColumnName(i),resultSet.getObject(i));
                    }
                    mapList.add(mapOfColumnValues);
                }
            handleLiPeiMXRecordNew(record,mapList,recordNew);
        }

        protected void handleLiPeiMXRecordNew(Record record,List<Map<String,Object>> mapList,Record recordNew){
            List<LisLipeiMingxiData> lisLipeiMingxiData = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                LisLipeiMingxiData lipeiMingxiData = new LisLipeiMingxiData();
                lipeiMingxiData.convert_Enaty(map);
                lisLipeiMingxiData.add(lipeiMingxiData);
            }
            for (LisLipeiMingxiData LisLipeiData : lisLipeiMingxiData) {
                handleLiPeiMXAddColumn(recordNew,LisLipeiData);
            }
        }

        /**
         *  组装理赔明细数据字段
         * @param recordNew
         * @param chengbao
         */
        protected void handleLiPeiMXAddColumn(Record recordNew,LisLipeiMingxiData lisLipeiData){
            recordNew.addColumn(new StringColumn(lisLipeiData.getId()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getClaim_id()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getReport_no()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getPremium()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getClaim_time()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getClaim_reason()));
        }

        /*****************************************************************************************/
        /*************************************** 理赔数据处理 ***************************************/

        protected void dataHandleLiPei(ResultSet resultSet,Record record,Record recordNew) throws SQLException {
            List<Map<String,Object>> mapList = new ArrayList<>();
            //没有查询到
            if (resultSet.getRow() == 0){
                LisLipeiData lisLipeiData = new LisLipeiData();
                lisLipeiData.convert_Enaty(record.getColumn(1).asString(),record.getColumn(2).asString(),record.getColumn(3).asString());
                handleLiPeiAddColumn(recordNew,lisLipeiData);
            }else {
                ResultSetMetaData meta = resultSet.getMetaData();
                int count = meta.getColumnCount();
                //将查询结果放入 mapList
                while (resultSet.next()){
                    Map mapOfColumnValues = new HashMap(count);
                    for (int i = 1; i <= count; i++) {
                        mapOfColumnValues.put(meta.getColumnName(i),resultSet.getObject(i));
                    }
                    mapList.add(mapOfColumnValues);
                }
                handleLiPeiRecordNew(record,mapList,recordNew);
            }
        }

        protected void handleLiPeiRecordNew(Record record,List<Map<String,Object>> mapList,Record recordNew){
            List<LisLipeiData> lisLipeiDataList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                LisLipeiData lisLipeiData = new LisLipeiData();
                lisLipeiData.convert_Enaty(map,record.getColumn(1).asString(),record.getColumn(2).asString(),record.getColumn(3).asString());
                lisLipeiDataList.add(lisLipeiData);
            }
            for (LisLipeiData lisLipeiData : lisLipeiDataList) {
                handleLiPeiAddColumn(recordNew,lisLipeiData);
            }
        }

        /**
         *  组装理赔数据字段
         * @param recordNew
         * @param chengbao
         */
        protected void handleLiPeiAddColumn(Record recordNew,LisLipeiData lisLipeiData){
            recordNew.addColumn(new StringColumn(lisLipeiData.getId()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getBatch_no()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getClaim_id()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getCommission_id()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getPeople_id()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getCorporate_name()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getResult_state()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getPolicy_no()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getPolicy_holder()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getPolicy_holder_id_card()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getInsured_name()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getInsured_id_card()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getInsurance_name()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getInsurance_type()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getAccumulate_premium()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getYear_premium()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getLast_claim_time()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getFirst_claim_time()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getReceiver_name()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getReceiver_phone()));
            recordNew.addColumn(new StringColumn(lisLipeiData.getReceiver_id_card()));
        }
        /*****************************************************************************************/
        /*************************************** 保全数据处理 ***************************************/

        protected void dataHandleBaoQuan(ResultSet resultSet,Record record,Record recordNew) throws SQLException {
            List<Map<String,Object>> mapList = new ArrayList<>();
            //没有查询到
            if (resultSet.getRow() == 0){
                LisBaoquanData baoquanData = new LisBaoquanData();
                baoquanData.convert_Enaty(record.getColumn(1).asString(),record.getColumn(2).asString(),record.getColumn(3).asString());
                handleBaoQuanAddColumn(recordNew,baoquanData);
            }else {
                ResultSetMetaData meta = resultSet.getMetaData();
                int count = meta.getColumnCount();
                //将查询结果放入 mapList
                while (resultSet.next()){
                    Map mapOfColumnValues = new HashMap(count);
                    for (int i = 1; i <= count; i++) {
                        mapOfColumnValues.put(meta.getColumnName(i),resultSet.getObject(i));
                    }
                    mapList.add(mapOfColumnValues);
                }
                handleChengBaoRecordNew(record,mapList,recordNew);
            }
        }

        protected void handleBaoQuanRecordNew(Record record,List<Map<String,Object>> mapList,Record recordNew){
            List<LisBaoquanData> baoquanDataList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                LisBaoquanData baoquanData = new LisBaoquanData();
                baoquanData.convert_Enaty(map,record.getColumn(1).asString(),record.getColumn(2).asString(),record.getColumn(3).asString());
                baoquanDataList.add(baoquanData);
            }
            for (LisBaoquanData baoquanData : baoquanDataList) {
                handleBaoQuanAddColumn(recordNew,baoquanData);
            }
        }

        /**
         *  组装保全数据字段
         * @param recordNew
         * @param chengbao
         */
        protected void handleBaoQuanAddColumn(Record recordNew,LisBaoquanData baoquan){
            recordNew.addColumn(new StringColumn(baoquan.getId()));
            recordNew.addColumn(new StringColumn(baoquan.getBatch_no()));
            recordNew.addColumn(new StringColumn(baoquan.getCommission_id()));
            recordNew.addColumn(new StringColumn(baoquan.getPeople_id()));
            recordNew.addColumn(new StringColumn(baoquan.getCorporate_name()));
            recordNew.addColumn(new StringColumn(baoquan.getResult_state()));
            recordNew.addColumn(new StringColumn(baoquan.getPolicy_holder()));
            recordNew.addColumn(new StringColumn(baoquan.getPolicy_no()));
            recordNew.addColumn(new StringColumn(baoquan.getPolicy_holder_id_card()));
            recordNew.addColumn(new StringColumn(baoquan.getInsurance_name()));
            recordNew.addColumn(new StringColumn(baoquan.getInsurance_type()));
            recordNew.addColumn(new StringColumn(baoquan.getAccumulate_premium()));
            recordNew.addColumn(new StringColumn(baoquan.getReturn_amount()));
            recordNew.addColumn(new StringColumn(baoquan.getReturn_time()));
            recordNew.addColumn(new StringColumn(baoquan.getLoan_amount()));
            recordNew.addColumn(new StringColumn(baoquan.getLoan_time()));
        }


        /*****************************************************************************************/
        /*************************************** 承保数据处理 ***************************************/

        /**
         *  承保信息处理
         * @param resultSet
         * @param record
         * @param recordNew
         * @throws SQLException
         */
        protected void dataHandleChengBao(ResultSet resultSet,Record record,Record recordNew) throws SQLException {
            List<Map<String,Object>> mapList = new ArrayList<>();
            //没有查询到
            if (resultSet.getRow() == 0){
                LisChengbao chengbao = new LisChengbao();
                chengbao.convert_Enaty(record.getColumn(1).asString(),record.getColumn(2).asString(),record.getColumn(3).asString());
                handleChengBaoAddColumn(recordNew,chengbao);
            }else {
                ResultSetMetaData meta = resultSet.getMetaData();
                int count = meta.getColumnCount();
                //将查询结果放入 mapList
                while (resultSet.next()){
                    Map mapOfColumnValues = new HashMap(count);
                    for (int i = 1; i <= count; i++) {
                        mapOfColumnValues.put(meta.getColumnName(i),resultSet.getObject(i));
                    }
                    mapList.add(mapOfColumnValues);
                }
                handleChengBaoRecordNew(record,mapList,recordNew);
            }
        }

        /**
         * 组装查询到的承保数据到 record
         * @param record
         * @param mapList
         * @param recordNew
         */
        protected void handleChengBaoRecordNew(Record record,List<Map<String,Object>> mapList,Record recordNew){
            List<LisChengbao> lisChengbaoList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                LisChengbao lisChengbao = new LisChengbao();
                lisChengbao.convert_Enaty(map,record.getColumn(1).asString(),record.getColumn(2).asString(),record.getColumn(3).asString());
                lisChengbaoList.add(lisChengbao);
            }
            for (LisChengbao chengbao : lisChengbaoList) {
                handleChengBaoAddColumn(recordNew,chengbao);
            }
        }

        /**
         *  组装承保数据字段
         * @param recordNew
         * @param chengbao
         */
        protected void handleChengBaoAddColumn(Record recordNew,LisChengbao chengbao){
            recordNew.addColumn(new StringColumn(chengbao.getId()));
            recordNew.addColumn(new StringColumn(chengbao.getBatch_no()));
            recordNew.addColumn(new StringColumn(chengbao.getCommission_id()));
            recordNew.addColumn(new StringColumn(chengbao.getPeople_id()));
            recordNew.addColumn(new StringColumn(chengbao.getCorporate_name()));
            recordNew.addColumn(new StringColumn(chengbao.getResult_state()));
            recordNew.addColumn(new StringColumn(chengbao.getPolicy_holder()));
            recordNew.addColumn(new StringColumn(chengbao.getPolicy_no()));
            recordNew.addColumn(new StringColumn(chengbao.getPolicy_holder_id_card()));
            recordNew.addColumn(new StringColumn(chengbao.getInsurance_name()));
            recordNew.addColumn(new StringColumn(chengbao.getInsurance_type()));
            recordNew.addColumn(new StringColumn(chengbao.getInsured_id_card()));
            recordNew.addColumn(new StringColumn(chengbao.getInsured_name()));
            recordNew.addColumn(new StringColumn(chengbao.getInsurance_status()));
            recordNew.addColumn(new StringColumn(chengbao.getFirst_pay_time()));
            recordNew.addColumn(new StringColumn(chengbao.getLast_pay_time()));
            recordNew.addColumn(new StringColumn(chengbao.getAccumulate_premium()));
            recordNew.addColumn(new StringColumn(chengbao.getSurrender_value()));
            recordNew.addColumn(new StringColumn(chengbao.getPay_method()));
            recordNew.addColumn(new StringColumn(chengbao.getYear_premium()));
            recordNew.addColumn(new StringColumn(chengbao.getStart_time()));
        }

        /**
         * @param recordSender
         * @param rs 查询到的数据
         * @param metaData
         * @param columnNumber
         * @param mandatoryEncoding
         * @param taskPluginCollector
         * @return
         */
        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                     TaskPluginCollector taskPluginCollector) {

            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if (StringUtils.isBlank(mandatoryEncoding)) {
                                rawData = rs.getString(i);
                            } else {
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY :
                                        rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if (rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            DBUtilErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    logger.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }
    }
}
