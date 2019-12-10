using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Web;
using Newtonsoft.Json;
using FunctionsLibrary;

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
                        SQLDB dbConnection = new SQLDB(parameterReader.ConnStrings[FileType]); 
                        dbConnection.openDB();
                        dbConnection.setCommand_StoredProcedure(parameterReader.StoredProc_dict[FileType]);

                        if (FileType == "fileType.TOPRS")
                        {
                            dbConnection.addParameters_Input("@ReportName", TOPRSReportName, SqlDbType.VarChar);
                            dbConnection.addParameters_Input("@ReportYear", TOPRSReportYear, SqlDbType.Int);
                            dbConnection.addParameters_Input("@ReportWeek", TOPRSReportWeek, SqlDbType.Int);
                            dbConnection.addParameters_Input("@IsTotal", TOPRSisTotal, SqlDbType.Int);
                        }
                        if (FileType == "fileType.SAP9502" || FileType == "fileType.SAP9532" || FileType == "fileType.SAP9502_WKLY" || FileType == "fileType.SAP9532_WKLY")
                        {
                            dbConnection.addParameters_Input("@ReportType", FileType, SqlDbType.VarChar);
                        }
                        dbConnection.addParameters_TableValued("@InsertData", InsertData, parameterReader.TypeName_dict[FileType]);
                        dbConnection.executeNonQuery();  //Insert data to DB. The stored procedure handles errors and logging

                        //log[1] = "File inserted";             
                    }
                    catch (Exception e)
                    {
                        //log[1] = "Error inserting file: " + e.ToString();    
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
            using (StreamReader r = new StreamReader("CSharpie\\SQL_StoredProcedures.json"))
            //using (StreamReader r = new StreamReader("SQL_StoredProcedures.json"))
            {
                string json = r.ReadToEnd();
                StoredProc_dict = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
            using (StreamReader r = new StreamReader("CSharpie\\SQL_TableTypes.json"))
            //using (StreamReader r = new StreamReader("SQL_TableTypes.json"))
            {
                string json = r.ReadToEnd();
                TypeName_dict = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
            using (StreamReader r = new StreamReader("CSharpie\\Parameters.json"))
            //using (StreamReader r = new StreamReader("Parameters.json"))
            {
                string json = r.ReadToEnd();
                Parameters = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
            using (StreamReader r = new StreamReader("CSharpie\\SQL_ConnStrings.json"))
            //using (StreamReader r = new StreamReader("SQL_ConnStrings.json"))
            {
                string json = r.ReadToEnd();
                ConnStrings = JsonConvert.DeserializeObject<Dictionary<string,string>>(json);
            }
        }
    }
}
