using System;
using System.Data;
using System.Data.SqlClient;

namespace FunctionsLibrary
{
    public class SQLDB
    {
        private bool isOpen { get; set; }
        private string connectionstring { get; set; }
        private SqlConnection sqlConnection { get; set; }
        private SqlCommand sqlCommand { get; set; }
        public SQLDB(string inputConnectionString)
        {
            connectionstring = inputConnectionString;
        }

        public void openDB()
        {
            SqlConnection NEWsqlconnection = new SqlConnection(connectionstring);
            SqlCommand NEWsqlCommand = new SqlCommand();

            sqlConnection = NEWsqlconnection;
            sqlCommand = NEWsqlCommand;

            sqlCommand = sqlConnection.CreateCommand();
            sqlConnection.Open();
            isOpen = true;
        }

        public void closeDB()
        {
            if (isOpen)
            {
                sqlConnection.Close();
                isOpen = false;
            }
        }


        public DataTable getData(bool needKey)
        {
            DataTable dtblResult = new DataTable();
            var adp = new SqlDataAdapter(sqlCommand);
            adp.Fill(dtblResult);

            if (needKey)
            {
                DataColumn[] key = new DataColumn[1];
                key[0] = dtblResult.Columns[0];
                dtblResult.PrimaryKey = key;
            }

            return dtblResult;
        }

        public object getScalar()
        {
            object output = 0;
            try { output = sqlCommand.ExecuteScalar(); }
            catch { }
            return output;
        }

        public void setCommand_CommandText(string sqlScript)
        {
            sqlCommand.CommandType = CommandType.Text;
            sqlCommand.CommandText = sqlScript;
        }

        public void setCommand_StoredProcedure(string storedProcName)
        {
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.Parameters.Clear();
            sqlCommand.CommandText = storedProcName;
        }

        public bool executeNonQuery()
        {
            try
            {
                sqlCommand.ExecuteNonQuery();
                return true;
            }
            catch
            {
                return false;
            };
        }

        public void addParameters_TableValued(string parameterName, DataTable InsertTable, string typeName)
        {
            SqlParameter AddTbl = sqlCommand.Parameters.AddWithValue(parameterName, InsertTable);  //Add the datatable as a table-valued parameter
            AddTbl.SqlDbType = SqlDbType.Structured;
            AddTbl.TypeName = typeName;
        }

        public void addParameters_Input(string parameterName, object parameterValue, SqlDbType inputType)
        {
            switch (inputType)
            {
                case SqlDbType.Int:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.Int);
                    try { sqlCommand.Parameters[parameterName].Value = Convert.ToInt32(parameterValue); }
                    catch { sqlCommand.Parameters[parameterName].Value = DBNull.Value; }
                    break;
                case SqlDbType.VarChar:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.VarChar);
                    try { sqlCommand.Parameters[parameterName].Value = Convert.ToString(parameterValue); }
                    catch { sqlCommand.Parameters[parameterName].Value = DBNull.Value; }
                    break;
                case SqlDbType.Decimal:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.Decimal);
                    try { sqlCommand.Parameters[parameterName].Value = Convert.ToDecimal(parameterValue); }
                    catch { sqlCommand.Parameters[parameterName].Value = DBNull.Value; }
                    break;
                case SqlDbType.Float:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.Float);
                    try { sqlCommand.Parameters[parameterName].Value = Convert.ToDouble(parameterValue); }
                    catch { sqlCommand.Parameters[parameterName].Value = DBNull.Value; }
                    break;
                case SqlDbType.DateTime:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.DateTime);
                    try { sqlCommand.Parameters[parameterName].Value = Convert.ToDateTime(parameterValue); }
                    catch { sqlCommand.Parameters[parameterName].Value = DBNull.Value; }
                    break;
            }
        }

        public void addParameters_Output(string parameterName, SqlDbType inputType)
        {
            switch (inputType)
            {
                case SqlDbType.Int:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.Int).Direction = ParameterDirection.Output;
                    break;
                case SqlDbType.VarChar:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.VarChar).Direction = ParameterDirection.Output;
                    break;
                case SqlDbType.Decimal:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.Decimal).Direction = ParameterDirection.Output;
                    break;
                case SqlDbType.Float:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.Float).Direction = ParameterDirection.Output;
                    break;
                case SqlDbType.DateTime:
                    sqlCommand.Parameters.Add(parameterName, SqlDbType.DateTime).Direction = ParameterDirection.Output;
                    break;
            }
        }

        public object setOutputValue(string parameterName)
        {
            return sqlCommand.Parameters[parameterName].Value;
        }

        public object executeScalar()
        {
            try
            {
                return sqlCommand.ExecuteScalar();
            }
            catch
            {
                return DBNull.Value;
            };
        }
    }

    public class ProcessLogger
    {
        public ProcessLogger(string Message, string Details, string ProcessName, string LogType, int Step, int DataID, string connectionString)
        {
            SQLDB Logger = new SQLDB(connectionString);
            Logger.openDB();

            Logger.setCommand_StoredProcedure("ProcessLog_OpenProcess");
            Logger.addParameters_Input("@DetailsIN", Details, SqlDbType.VarChar);
            Logger.addParameters_Input("@ProcessNameIN", ProcessName, SqlDbType.VarChar);
            Logger.addParameters_Input("@MessageIN", Message, SqlDbType.VarChar);
            Logger.addParameters_Input("@LogTypeIN", LogType, SqlDbType.VarChar);
            Logger.addParameters_Input("@ProcessStepIN", Step, SqlDbType.Int);
            Logger.addParameters_Input("@DataIDIN", DataID, SqlDbType.Int);

            bool isRan = Logger.executeNonQuery();
        }
    }
}
