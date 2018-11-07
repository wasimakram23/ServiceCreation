using System;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.IO;

namespace GroupMapping
{
    public class DbContext
    {
        string connectionString = "Data Source=(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST = salesdb.grameenphone.com)(PORT = 1521)))(CONNECT_DATA =(SERVICE_NAME = SALESDB)));User ID=TESTDMS;Password=welcome123;";
        OracleConnection connection;
        OracleTransaction transaction;
        OracleCommand command;

        public void startMapping()
        {
            using (connection = new OracleConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                    command = connection.CreateCommand();
                    command.Transaction = transaction;

                    try
                    {
                        WriteToFile("Mapping started at " + DateTime.Now, "MappingLog_");
                        command.CommandText = "insert into DMS_GROUPS(GROUPID,GROUPNAME,DESCRIPTION,CREATEDDATE) select temp1.id,temp1.role,temp1.description,temp1.UPDATEDATE from temp1 where id not in (select did from tempmap)";
                        command.ExecuteNonQuery();
                        command.CommandText = "update DMS_GROUPS SET CREATEDBY='autogen',ISACTIVE=1  where CREATEDBY is null";
                        int row= command.ExecuteNonQuery();
                        WriteToFile("Group Table Updating....\nNumber of new role = "+row, "MappingLog_");
                        command.CommandText = "insert into tempmap(did) select id from temp1 where id not in (select did from tempmap)";
                        command.ExecuteNonQuery();
                        command.CommandText = "update tempmap a set a.rrole=(select role from temp1 b where b.id=a.did),a.drole=(select role from temp1 c where c.id=a.did) where  a.rrole is null";
                        row = command.ExecuteNonQuery();
                        WriteToFile("Mapping Table Updating....\nNumber of new map = "+row, "MappingLog_");
                        command.CommandText = "UPDATE temp1 a SET a.role = (SELECT rrole FROM tempmap b WHERE b.drole = a.role )";
                        command.ExecuteNonQuery();
                        command.CommandText = "delete from DMS_USERSINGROUP where exists(select temp1.userid from temp1 where temp1.userid = DMS_USERSINGROUP.USERID)";
                        command.ExecuteNonQuery();
                        command.CommandText = "insert into DMS_USERSINGROUP select DMS_GROUPS.GROUPID,temp1.userid from temp1 , DMS_GROUPS where temp1.role = DMS_GROUPS.GROUPNAME";
                        row = command.ExecuteNonQuery();
                        WriteToFile("User Table Updating....\nNumber of affected row = "+row, "MappingLog_");
                        transaction.Commit();
                        Console.WriteLine("Mapping complete");
                        WriteToFile("Mapping Completed at "+DateTime.Now, "MappingLog_");
                    }
                    catch (Exception e)
                    {
                        transaction.Rollback();
                        Console.WriteLine(e.ToString());
                        Console.WriteLine("Mapping Failed due to some reason");
                        WriteToFile("Mapping failed at " + DateTime.Now + " due to,", "MappingLog_");
                        WriteToFile("Mapping failed at "+DateTime.Now+" due to,", "ErrorLog_");
                        WriteToFile(e.ToString(), "ErrorLog_");
                    }
                }
                catch (Exception ex) {
                    Console.WriteLine(ex.ToString());
                    WriteToFile("Connection failed due to,", "ErrorLog_");
                    WriteToFile(ex.ToString(), "ErrorLog_");
                } 
            }
        }

        private void WriteToFile(string Message,string fileSelector)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\"+fileSelector+"" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.   
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }
    }
}
