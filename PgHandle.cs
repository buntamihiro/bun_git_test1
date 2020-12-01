using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Npgsql;
using Oracle.DataAccess.Client;

namespace SchemaInfo
{
    class PgHandle
    {

        private DataSet PostgresCommand(NpgsqlConnection con, string str)
        {
            NpgsqlCommand command = new NpgsqlCommand(str, con);                      
            NpgsqlDataAdapter adapter = new NpgsqlDataAdapter();
            adapter.SelectCommand = command;
            DataSet ds = new DataSet();
            con.Open();     // ★★★
            adapter.Fill(ds);
            con.Close();    // ★★★
            return ds;
        }







        private DataTable SendCommand(NpgsqlConnection con, string cmd)
        {
            DataSet pds1 = PostgresCommand(con, cmd);

            DataTable pdt1 = pds1.Tables[0];
            return (pdt1);
        }



        public void ExecuteNonQuery(NpgsqlConnection conn, string sql)
        {
            using (NpgsqlCommand command = new NpgsqlCommand())
            {
                conn.ConnectionString = @"Server=localhost;Port=5432;User Id=admin;Password=admin;Database=TestDatabase;";

                // トランザクションを開始します。
                conn.Open();
                NpgsqlTransaction transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    command.CommandText = sql;
                    command.Connection = conn;
                    command.Transaction = transaction;
                    command.ExecuteNonQuery();

                    //トランザクションをコミットします。
                    transaction.Commit();
                }
                catch (System.Exception)
                {
                    //トランザクションをロールバックします。
                    transaction.Rollback();
                    throw;
                }
                finally
                {
                    conn.Close();
                }
            }
        }






        public DataTable LoadTableNameFromDB(NpgsqlConnection con)
        {
            string cmd = "SELECT relname FROM pg_stat_user_tables ORDER BY relname";
            return (SendCommand(con,cmd));
        }



        public DataTable LoadTableDataTypeFromDB(NpgsqlConnection con,string tableName)
        {
            string cmd = "SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_name = '" + tableName + "' order by column_name";
            return (SendCommand(con,cmd));
        }



        public DataTable LoadSeqNameFromDB(NpgsqlConnection con)
        {
            string cmd = "SELECT c.relname FROM pg_class c LEFT join pg_user u ON c.relowner = u.usesysid WHERE c.relkind = 'S';";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadViewInfoFromDB(NpgsqlConnection con)
        {
            string cmd = "SELECT viewname, definition FROM pg_views where schemaname = 'public';";
            return (SendCommand(con, cmd));
        }



        public DataTable LoadIndexInfoFromDB(NpgsqlConnection con)
        {
            string cmd = "SELECT tablename, indexname, indexdef FROM pg_indexes where schemaname = 'public';";
            return (SendCommand(con, cmd));
        }


        public DataTable LoadFunctionInfoFromDB(NpgsqlConnection con)
        {
            string cmd = "SELECT p.proname AS function_name,pg_get_function_arguments(p.oid) AS args, pg_get_functiondef(p.oid)AS func_def ";
            cmd = cmd + "FROM(SELECT oid, *FROM pg_proc p WHERE NOT p.proisagg) p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' ;";                       
            return (SendCommand(con, cmd));
        }


        public DataTable LoadStatActivityFromDB(NpgsqlConnection con)
        {
            //string cmd = "SELECT datid,datname,pid,usesysid,usename,application_name,client_addr,client_port,backend_start,query_start,state_change,state,query ";
            string cmd = "SELECT datid,datname,pid,usesysid,usename,application_name,client_addr,client_port,backend_start,query_start,state_change,state ";
            cmd = cmd + "FROM pg_stat_activity";
            return (SendCommand(con, cmd));
        }


    }


}
