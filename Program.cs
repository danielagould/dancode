using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Web;
using Newtonsoft.Json;

namespace VS
{
    class Program
    {
        // dotnet publish -c Release -r win10-x64
        static void Main(string[] args)
        {
            ParameterReader parameterReader = new ParameterReader();
            //string upLoadFile = parameterReader.Parameters["UploadFile"];
            string [] allFiles = Directory.GetFiles(parameterReader.Parameters["FileDropFolder"]);
            
            string[] log = {"",""};

            foreach (string upLoadFile in allFiles)
            {
                DataTable InsertData = new DataTable();
                string FileType =  "";
                string TOPRSReportName = "";
                string TOPRSReportYear = "";
                string TOPRSReportWeek = "";
                string TOPRSisTotal = "";
                if (Path.GetExtension(upLoadFile) == ".csv")
                {
                    try
                    {
                        using(var reader = new StreamReader(upLoadFile))
                        {
                            while (!reader.EndOfStream)
                            {
                                var line = reader.ReadLine();
                                var values = line.Split('|');
                                if (values[0] == "#HEADER")
                                {
                                    FileType = values[1];
                                    if (FileType == "fileType.TOPRS")
                                    {
                                        TOPRSReportName = values[2];
                                        TOPRSReportYear = values[3];
                                        TOPRSReportWeek = values[4];
                                        TOPRSisTotal = values[5];
                                    }
                                }
                                else
                                {
                                    if (values.Length > InsertData.Columns.Count)
                                    {
                                        for (int i = InsertData.Columns.Count; i < values.Length; i++)
                                        {
                                            InsertData.Columns.Add();
                                        }
                                    }
                                    if (values.Length > 1)
                                    {
                                        InsertData.Rows.Add(values);
                                    }
                                }
                            }
                        }
                        log[0] = "File read to memory";
                        
                    }
                    catch (Exception e)
                    {
                        log[0] = "Failure to read file: " + e.ToString();
                    }
                    
                    try
                    {
                        SqlConnection connection = new SqlConnection(parameterReader.ConnStrings[FileType]);
                        SqlCommand command = new SqlCommand("", connection);
                        connection.Open();
                        command.CommandType = CommandType.StoredProcedure;

                        command.CommandText = parameterReader.StoredProc_dict[FileType]; //Stored procedure for inserting data
                        if (FileType == "fileType.TOPRS")
                        {
                            command.Parameters.Add("@ReportName", SqlDbType.NVarChar);
                            command.Parameters["@ReportName"].Value = TOPRSReportName;
                            command.Parameters.Add("@ReportYear", SqlDbType.Int);
                            command.Parameters["@ReportYear"].Value = TOPRSReportYear;
                            command.Parameters.Add("@ReportWeek", SqlDbType.Int);
                            command.Parameters["@ReportWeek"].Value = TOPRSReportWeek;
                            command.Parameters.Add("@IsTotal", SqlDbType.Int);
                            command.Parameters["@IsTotal"].Value = TOPRSisTotal;
                        }
                        SqlParameter insertData = command.Parameters.AddWithValue("@InsertData", InsertData);  //Add the datatable as a table-valued parameter
                        insertData.SqlDbType = SqlDbType.Structured;
                        insertData.TypeName = parameterReader.TypeName_dict[FileType];

                        command.ExecuteNonQuery();  //Insert data to DB. The stored procedure handles errors and logging
                    
                        log[1] = "File inserted";             
                    }
                    catch (Exception e)
                    {
                        log[1] = "Error inserting file: " + e.ToString();    
                    }
                    File.Delete(upLoadFile);

                    //System.IO.File.WriteAllLines("Output\\LogFile.txt", log);
                }
            }
        }   
    }

    class ParameterReader
    {
        public ParameterReader()
        {
            AssignDictionaries();
        }
        public Dictionary<string, string> StoredProc_dict = new Dictionary<string, string>();
        public Dictionary<string, string> TypeName_dict = new Dictionary<string, string>();
        public Dictionary<string, string> Parameters = new Dictionary<string, string>();
        public Dictionary<string, string> ConnStrings = new Dictionary<string, string>();
        public void AssignDictionaries()
        {   
            //CSharpie\\
            using (StreamReader r = new StreamReader("CSharpie\\SQL_StoredProcedures.json"))
            {
                string json = r.ReadToEnd();
                StoredProc_dict = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
            using (StreamReader r = new StreamReader("CSharpie\\SQL_TableTypes.json"))
            {
                string json = r.ReadToEnd();
                TypeName_dict = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
            using (StreamReader r = new StreamReader("CSharpie\\Parameters.json"))
            {
                string json = r.ReadToEnd();
                Parameters = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
            using (StreamReader r = new StreamReader("CSharpie\\SQL_ConnStrings.json"))
            {
                string json = r.ReadToEnd();
                ConnStrings = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
        }
    }
}
