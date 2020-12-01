using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Npgsql;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;


namespace SchemaInfo
{
    class OrHandle
    {


        public DataSet OracleCommand(OracleConnection con, string str)
        {
            OracleCommand command = new OracleCommand(str, con);
            OracleDataAdapter adapter = new OracleDataAdapter();
            adapter.SelectCommand = command;
            DataSet ds = new DataSet();
            //con.Open();
            adapter.Fill(ds);
            //con.Close();
            return ds;
        }




        private DataTable SendCommand(OracleConnection con, string cmd)
        {
            DataSet pds1 = OracleCommand(con, cmd);

            DataTable pdt1 = pds1.Tables[0];
            return (pdt1);
        }






        public DataTable LoadTableInfoFromDB(OracleConnection con)
        {
            string cmd = "SELECT TC.TABLE_NAME, TC.COLUMN_NAME, TC.DATA_TYPE FROM USER_TAB_COLUMNS TC ORDER BY TC.TABLE_NAME, TC.COLUMN_NAME";
            return (SendCommand(con, cmd));
        }




        public DataTable LoadSeqNameFromDB(OracleConnection con)
        {
            string cmd = "SELECT SEQUENCE_NAME FROM USER_SEQUENCES ORDER BY SEQUENCE_NAME";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadViewInfoFromDB(OracleConnection con)
        {
            string cmd = "SELECT V.VIEW_NAME, V.TEXT FROM USER_VIEWS V ORDER BY V.VIEW_NAME";
            //string cmd = "SELECT views.VIEW_NAME AS view_name,views.TEXT AS definition FROM USER_VIEWS views ORDER BY views.VIEW_NAME";
            //string cmd = "select view_name, text from user_views where view_name IN(SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'VIEW')";
            //string cmd = "select view_name, text from user_views where view_name = 'CHART_VW_SPC'";
            //string cmd = "SELECT NAME,LINE,TEXT FROM USER_SOURCE WHERE NAME IN (SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'VIEW') ORDER BY NAME,LINE";
            return (SendCommand(con, cmd));
        }



        public DataTable LoadIndexInfoFromDB(OracleConnection con)
        {
            string cmd = "SELECT TABLE_NAME,COLUMN_NAME,INDEX_NAME FROM user_ind_columns order by table_name";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadFunctionInfoFromDB(OracleConnection con)
        {
            string cmd = "SELECT NAME,LINE,TEXT FROM USER_SOURCE WHERE NAME IN (SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'FUNCTION') ORDER BY NAME,LINE";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadProcedureInfoFromDB(OracleConnection con)
        {
            string cmd = "SELECT NAME,LINE,TEXT FROM USER_SOURCE WHERE NAME IN (SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PROCEDURE') ORDER BY NAME,LINE";
            return (SendCommand(con, cmd));
        }



        public DataTable LoadTriggerInfoFromDB(OracleConnection con)
        {
            string cmd = "SELECT NAME,LINE,TEXT FROM USER_SOURCE WHERE NAME IN (SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TRIGGER') ORDER BY NAME,LINE";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadPackageInfoFromDB(OracleConnection con)
        {
            //string cmd = "SELECT NAME,LINE,TEXT FROM USER_SOURCE WHERE NAME IN (SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PACKAGE') ORDER BY NAME,LINE";
            string cmd = "SELECT NAME,LINE,TEXT FROM USER_SOURCE WHERE NAME IN (SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PACKAGE') ORDER BY NAME";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadSchedulerInfoFromDB(OracleConnection con)
        {
            
            string cmd = "select job_name,owner,enabled,comments,logging_level,job_class,schedule_name,job_priority,job_weight from sys.dba_scheduler_jobs; ";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadObjectInfoFromDB(OracleConnection con,string objectName)
        {
            string cmd = "SELECT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = '" + objectName +"'";
            return (SendCommand(con, cmd));
        }


    }
}
