using System.Data;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Collections;

namespace DB_TASK
{
    //Simple class for checks and error msg management
    class Check
    {
        private string message = "";
        private bool valid = true;
        public void SetMessage(string str)
        {
            message = str;
        }
        public void SetResult(bool r)
        {
            valid = r;
        }
        public string GetMessage()
        {
            return message;
        }
        public bool GetResult()
        {
            return valid;
        }
    }

    class SqlManager //class to deal with sql
    {
        private TableHelper helper = new TableHelper();

        public string GetConnection() //Simple method to get the connection string from appsettings.json
        {
            System.Console.WriteLine("Retrieving connection string");
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            IConfigurationRoot configuration = builder.Build();
            System.Console.WriteLine("-- Done");

            return configuration.GetConnectionString("DB").ToString();
        }
        public void FillTable(DataTable dt, DataTable dt_invalid, string cs)
        {
            // Function that executes 2 bulk inserts into the db. One for substances and one for invalid data.
            using (SqlConnection connection = new SqlConnection(cs))
            {
                connection.Open();
                System.Console.WriteLine("Trying to insert substances");
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {

                    bulkCopy.DestinationTableName = "Substances";
                    try
                    {
                        bulkCopy.ColumnMappings.Add("SPElemFlow", "SPElemFlow");
                        bulkCopy.ColumnMappings.Add("SPUnit", "SPUnit");
                        bulkCopy.ColumnMappings.Add("SPMainCompartment", "SPMainCompartment");
                        bulkCopy.ColumnMappings.Add("FlowId", "FlowId");
                        bulkCopy.ColumnMappings.Add("ConversionFactor", "ConversionFactor");
                        bulkCopy.WriteToServer(dt);
                        System.Console.WriteLine("--Done");
                    }
                    catch (Exception e)
                    {
                        System.Console.WriteLine(e.Message);
                    }
                }
                System.Console.WriteLine("Trying to insert Invalid data");
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "InvalidData";
                    try
                    {
                        bulkCopy.ColumnMappings.Add("ErrorType", "ErrorType");
                        bulkCopy.ColumnMappings.Add("ErrorDescription", "ErrorDescription");
                        bulkCopy.ColumnMappings.Add("Data", "Data");
                        bulkCopy.WriteToServer(dt_invalid);
                        System.Console.WriteLine("--Done");
                    }
                    catch (Exception e)
                    {
                        Console.Write(e.Message);
                        System.Console.WriteLine(e.Message);
                    }
                }
            }
        }

        public void ExecuteProcedures(string cs)
        {
            //Function that executes the 4 stored procedure in the db
            //For each procedure, executes the command as ExecuteReader to show results.
            //Use the command result to create temporary datatables (check that types and positions of columns are as expected)
            //Use temporary dts to print result to console.
            var procedures = new List<int> { 1, 2, 3, 4 };
            using (var conn = new SqlConnection(cs))
            {
                conn.Open();
                foreach (int procedura in procedures)
                {
                    System.Console.WriteLine(String.Format("Beginning procedure {0}", procedura));
                    using (var command = new SqlCommand("procedure" + procedura.ToString(), conn)
                    {
                        CommandType = CommandType.StoredProcedure
                    })
                    {
                        DataTable result = helper.CreateResultTable(procedura);
                        var res = command.ExecuteReader();
                        if (res.HasRows) //Result not empty
                        {
                            System.Console.WriteLine("-- Result not empty");
                            System.Console.WriteLine("");
                            while (res.Read())
                            {
                                result = helper.FillResultTable(result, procedura, res); //Create dt from result
                            }

                            System.Console.WriteLine(String.Format(("Procedure {0}"), procedura)); //print result
                            helper.printDataTable(result);
                        }
                        else
                        {
                            System.Console.WriteLine("-- Result empty");
                            System.Console.WriteLine("");
                        }

                        res.Close(); //close the reader!
                    }
                }
            }
        }
    }

    class TableHelper //class for datatable utilities
    {
        public void ExportXml(DataTable dt, string FilePath, string TableName)
        {
            //Function used to export a datatable to a certain xml file 
            System.Console.WriteLine(String.Format("Exporting datatable: {0}", TableName));
            DataSet ds = new DataSet(TableName + "_table"); //First convert to dataset --> I think the output is more readable
            dt.TableName = TableName + "_row";
            ds.Tables.Add(dt);
            string dsXml = ds.GetXml();
            using (StreamWriter fs = new StreamWriter(FilePath))
            {
                ds.WriteXml(fs);
            }
        }
        public void printDataTable(DataTable tbl)
        {
            // Simple function to print a datatable in a "pretty" way: titles and columns
            string line = "";
            foreach (DataColumn item in tbl.Columns)
            {
                line += item.ColumnName + "   ";
            }
            line += "\n";
            foreach (DataRow row in tbl.Rows)
            {
                for (int i = 0; i < tbl.Columns.Count; i++)
                {
                    line += row[i].ToString() + "   ";
                }
                line += "\n";
            }
            Console.WriteLine(line);
        }

        public DataTable CreateResultTable(int procedure)
        {
            //Function that creates the datatable for a certain procedure. We enforce names and types to better check results.
            DataTable dt = new DataTable(); ;
            switch (procedure)
            {
                case 1:
                    dt.Columns.Add("SPElemFlow", typeof(string));
                    dt.Columns.Add("N_OCCURRENCES", typeof(int));
                    break;
                case 2:
                    dt.Columns.Add("Id", typeof(int));
                    dt.Columns.Add("SPElemFlow", typeof(string));
                    dt.Columns.Add("SPUnit", typeof(string));
                    dt.Columns.Add("SPMainCompartment", typeof(string));
                    dt.Columns.Add("FlowId", typeof(string));
                    dt.Columns.Add("ConversionFactor", typeof(Decimal));
                    break;
                case 3:
                    dt.Columns.Add("ErrorType", typeof(int));
                    dt.Columns.Add("N_OCCURRENCES", typeof(int));
                    break;
                case 4:
                    dt.Columns.Add("GEO", typeof(string));
                    break;
            }
            return dt;
        }

        public DataTable FillResultTable(DataTable dt, int procedure, SqlDataReader res)
        {
            //Function used to build the datatables rows starting from the result of the procedure.
            DataRow dr = dt.NewRow();
            switch (procedure)
            {
                case 1:
                    dr[0] = res.GetString(0);
                    dr[1] = res.GetInt32(1);
                    break;
                case 2:
                    dr[0] = res.GetInt32(0);
                    dr[1] = res.GetString(1);
                    dr[2] = res.GetString(2);
                    dr[3] = res.GetString(3);
                    dr[4] = res.GetString(4);
                    dr[5] = res.GetDecimal(5);
                    break;
                case 3:
                    dr[0] = res.GetInt32(0);
                    dr[1] = res.GetInt32(1);
                    break;
                case 4:
                    dr[0] = res.GetString(0);
                    break;
            }
            dt.Rows.Add(dr);
            return dt;
        }
    }

    class SimpleTests //class for simple tests
    {

        public bool ExecuteTests(string path, DataTable dt, DataTable dt_invalid) //callable function for all tests
        {
            //might do a single if, but this "list" is a bit easier to debug.
            if (!CheckGUID(dt))
            {
                Console.WriteLine("GUID test not ok");
                return false;
            }
            if (!CheckNulls(dt))
            {
                Console.WriteLine("Check null test not ok for dt");
                return false;
            }
            if (!CheckNulls(dt_invalid))
            {
                Console.WriteLine("Check null test not ok for dt_invalid");
                return false;
            }
            if (!CheckErrorTypes(dt_invalid))
            {
                Console.WriteLine("Error types test not ok");
                return false;
            }
            return true;
        }

        public static bool CheckGUID(DataTable dt) //check unique guids
        {
            List<Guid> list = new List<Guid>();
            List<Guid> list_unique = new List<Guid>();
            foreach (DataRow row in dt.Rows)
            {
                list.Add((Guid)row[3]);
            }
            list_unique = list.Distinct().ToList();
            if (list_unique.Count == list.Count)
            {
                return true;
            }
            return false;
        }

        public static bool CheckNulls(DataTable dt) //check null values
        {
            bool hasnull = false;
            foreach (DataColumn column in dt.Columns)
            {
                if (dt.Rows.OfType<DataRow>().Any(r => r.IsNull(column)))
                    hasnull = true;
            }
            if (hasnull)
            {
                return false;
            }
            return true;
        }

        public static bool CheckErrorTypes(DataTable dt) //check error type (in dt_invalid)
        {
            foreach (DataRow row in dt.Rows)
            {
                if ((int)row[0] != 0)
                {
                    return false;
                }
            }
            return true;
        }

    }

    class InputHelper //class for input from csv to datatable
    {
        public static Check Check_Values(string spelemflow, string spunit, string spmaincompartment, string flowid, string conversionfactor)
        {
            // I created this function to check for missing values using a custom class I created earlier
            Check result = new Check();
            bool error = false;
            string error_msg = "";
            if (String.IsNullOrEmpty(spelemflow))
            {
                error = true;
                error_msg += "missing spelemflow ";
            }
            if (String.IsNullOrEmpty(spunit))
            {
                error = true;
                error_msg += "missing spunit ";
            }
            if (String.IsNullOrEmpty(spmaincompartment))
            {
                error = true;
                error_msg += "missing spmaincompartment ";
            }
            if (String.IsNullOrEmpty(flowid))
            {
                error = true;
                error_msg += "missing flowid ";
            }
            if (String.IsNullOrEmpty(conversionfactor))
            {
                error = true;
                error_msg += "missing conversionfactor ";
            }
            result.SetResult(!error);
            result.SetMessage(error_msg);
            return result;
        }


        public static string[] FixRowExtraction(string row)
        {
            // Core of data extraction. For each row of csv, executes a great deal of string manipulatin in order to fix most of the input incosistencies
            string[] result = new string[5]; //prepare output
            int first_quotes = 0;
            int first_comma = 0;
            int second_quotes = 0;
            string table_cell = "";
            int i = 0;
            while (!String.IsNullOrEmpty(row)) //check that string is not empty
            {
                first_quotes = row.IndexOf("\"");
                first_comma = row.IndexOf(",");
                if (i == 4) //last "word" of the row --> it is a decimal number, so only "." should be allowed
                {
                    table_cell = row.Replace(",", ".").Replace("\"", "");
                    row = "";
                }

                else if ((first_quotes < first_comma) && (first_quotes >= 0)) //quoted text, we need to extract it
                {
                    row = row.Substring(first_quotes + 1, row.Length - 1); //extract from first quotes
                    second_quotes = row.IndexOf("\"");

                    if ((i == 4) && (row.IndexOf(",") >= 0)) //if last word, also check that only "." is present
                    {
                        row = row.Replace(",", ".");
                    }

                    table_cell = row.Substring(0, row.IndexOf("\"")); //extract field from string
                    row = row.Substring(second_quotes + 1, row.Length - table_cell.Length - 1); //update the row without the extracted field

                    if ((!String.IsNullOrEmpty(row)) && (row.Substring(0, 1) == ",")) //sometimes after quotes we have a comma,so delete it
                    {
                        row = row.Substring(1, row.Length - 1);
                    }
                }
                else if (first_comma >= 0) //case of no quoted text: simple comma management
                {
                    table_cell = row.Substring(0, first_comma);
                    row = row.Substring(first_comma + 1, row.Length - table_cell.Length - 1);
                }
                result[i] = table_cell;
                i++;
            }
            if (result[4] is null) //sometimes the last word is missing!
            {
                result[4] = "";
            }
            return result;
        }

        public DataTable[] ConvertCSVtoDataTable(string strFilePath)
        {
            //Function to extract data from csv
            DataTable[] result = new DataTable[2];

            DataTable dt = new DataTable(); //dt for substances


            DataTable dt_invalid = new DataTable(); //dt for invalid data
            dt_invalid.Columns.Add("ErrorType", typeof(int));
            dt_invalid.Columns.Add("ErrorDescription", typeof(string));
            dt_invalid.Columns.Add("Data", typeof(string));

            //Headers
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                string[] headers = sr.ReadLine().Split(',');
                foreach (string header in headers)
                {
                    if (header.Equals("FlowId"))
                    {
                        dt.Columns.Add(header, typeof(Guid));
                    }
                    else
                    {
                        if (header.Equals("ConversionFactor"))
                        {
                            dt.Columns.Add(header, typeof(Decimal));
                        }
                        else
                        {
                            dt.Columns.Add(header, typeof(string));
                        }

                    }

                }
                System.Console.WriteLine("Headers read");
                System.Console.WriteLine("Reading rows and doing tweaks, might take some time");
                int rownumber = 1;
                string row = "";
                while (!sr.EndOfStream) //read all rows
                {
                    rownumber++;
                    row = sr.ReadLine();
                    DataRow dr = dt.NewRow();
                    string[] table_cells = new string[5];
                    table_cells = FixRowExtraction(row); //extract the 5 fields
                    Check check = Check_Values(table_cells[0].ToString(), table_cells[1].ToString(), table_cells[2].ToString(), table_cells[3].ToString(), table_cells[4].ToString()); //check the 5 fields
                    if (check.GetResult()) //No error
                    {
                        for (int i = 0; i < 4; i++) //update datarow: careful about some strange Decimal formats to manage
                        {
                            dr[i] = table_cells[i];
                        }
                        if (table_cells[4].Contains("e") || table_cells[4].Contains("E"))
                        {
                            dr[4] = Double.Parse(table_cells[4], System.Globalization.NumberStyles.Any);
                        }
                        else
                        {
                            dr[4] = Convert.ToDecimal(table_cells[4]);
                        }
                        dt.Rows.Add(dr); //update datatable
                    }
                    else //Error --> missing value!
                    {
                        dr = dt_invalid.NewRow();
                        dr[0] = 0;
                        dr[1] = check.GetMessage();
                        dr[2] = String.Format("Row {0} of csv", rownumber);
                        dt_invalid.Rows.Add(dr);
                    }
                }

            }
            dt = RemoveDuplicateRows(dt, "FlowId"); //Very important to avoid duplicates!
            result[0] = dt;
            result[1] = dt_invalid;
            return result;
        }

        public static DataTable RemoveDuplicateRows(DataTable dt, string columnName)
        {
            Hashtable ht = new Hashtable();
            ArrayList duplicates = new ArrayList();

            //Add list of all the unique item value to hashtable, which stores combination of key, value pair.
            //And add duplicate item value in arraylist.
            foreach (DataRow drow in dt.Rows)
            {
                if (ht.Contains(drow[columnName]))
                    duplicates.Add(drow);
                else
                    ht.Add(drow[columnName], string.Empty);
            }

            //Removing a list of duplicate items from datatable.
            foreach (DataRow dRow in duplicates)
                dt.Rows.Remove(dRow);

            //Datatable which contains unique records will be return as output.
            return dt;
        }

    }

    //Program class to execute the app
    class Program
    {
        static void Main(string[] args)
        {
            SqlManager manager = new SqlManager();
            TableHelper t_helper = new TableHelper();
            InputHelper i_helper = new InputHelper();
            SimpleTests tester = new SimpleTests();
            string in_path = "Substances.csv";
            //Main method: sequence of steps
            System.Console.WriteLine("Beginning!");

            //Get connection string
            string cs = manager.GetConnection();

            //Read data
            DataTable[] dts = i_helper.ConvertCSVtoDataTable(in_path);
            DataTable dt = dts[0];
            DataTable dt_invalid = dts[1];
            System.Console.WriteLine("Done reading!");

            //Insert data
            manager.FillTable(dt, dt_invalid, cs);
            System.Console.WriteLine("Done with the insert!");

            //Export data
            t_helper.ExportXml(dt, "Substances.xml", "Substances");
            t_helper.ExportXml(dt_invalid, "InvalidData.xml", "InvalidData");
            System.Console.WriteLine("Done exporting!");


            // Run simple tests
            bool result = tester.ExecuteTests(in_path, dt, dt_invalid);
            if (!result)
            {
                throw new Exception("Tests failed");
            }
            System.Console.WriteLine("Test ok");

            //Execute procedures
            manager.ExecuteProcedures(cs);
            System.Console.WriteLine("Done with the procedures!");

            System.Console.WriteLine("Finished!");
        }
    }
}
