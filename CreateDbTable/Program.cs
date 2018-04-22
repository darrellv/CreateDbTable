using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CreateDbTable.Classes;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;



namespace CreateDbTable
{
    class Program
    {
        private static List<string> errorList = new List<string>();

        static void Main(string[] args)
        {
            //make sure no tables already exist in the database
            if (DatabaseTablesExist())
            {
                ReportError("Tables already exist in database.","");
            }
            else
            {
                //setup some variables we will use throughout the program
                List<string> fileContents = new List<string>();
                string tableName = "";
                List<string[]> rows = new List<string[]>();

                //make sure they added the filename
                if (args == null || args.Length == 0)
                {
                    Console.WriteLine("Please specify a filename as a parameter.");
                    return;
                }


                try
                {
                    //read the data file into a list of string.
                    fileContents = File.ReadAllLines(args[0]).ToList<string>();
                }
                catch (FileNotFoundException fileex)
                {
                    ReportError(fileex.Message, fileex.StackTrace);
                }
                catch (Exception ex)
                {
                    ReportError(ex.Message, ex.StackTrace);
                }

                List<ColumnAttributes> colInfo = new List<ColumnAttributes>();

                //set up the column information and parse out the input rows of the input file
                ProcessTableData(fileContents, ref tableName, ref rows, ref colInfo);

                //use the column information and table name to generate the create table script.
                string SqlCreateTableScript = GenerateCreateTableScript(tableName, colInfo);

                //create the table and if it was created ok, then go ahead and insert the data.
                if (CreateTable(SqlCreateTableScript))
                {
                    InsertData(tableName, rows, colInfo);
                }

            }

            //if there were any errors reported, append them to the error log file.
            if (errorList.Count > 0)
            {
                WriteErrorLog();
            }
            Console.WriteLine("Program Completed");


        }

        private static void WriteErrorLog()
        {
            using (TextWriter tw = new StreamWriter("ErrorLog.txt", true))
            {
                foreach (String s in errorList)
                    tw.WriteLine(s);
            }
        }

        private static void ReportError(string message, string stackTrace)
        {
            errorList.Add(string.Format("{0}, {1}, {2}", DateTime.Now.ToString(), message, stackTrace));
        }

        private static void InsertData(string tableName, List<string[]> rows, List<ColumnAttributes> colInfo)
        {

            //set up the first part of the insert statement.  we do this once and will reuse it.
            string sqlInsertCols = string.Format("insert into {0} (", tableName);
            foreach (ColumnAttributes ca in colInfo)
            {
                sqlInsertCols += string.Format("{0}{1},", Environment.NewLine, ca.Name);
            }
            sqlInsertCols = sqlInsertCols.TrimEnd(',') + ") " + Environment.NewLine;

            //get the connection ready and open it.  we will open one connection and use that to insert
            //all the data.
            string conn = ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;

            using (SqlConnection sc = new SqlConnection(conn))
            {
                sc.Open();

                //for every row in the input data, set up the 2nd part of the sql statement and insert the row.
                foreach (string[] fields in rows)
                {
                    string sqlInsertRow = "values (";
                    sqlInsertRow += Environment.NewLine + string.Join(",", fields) + ")";
                    string insertStatement = sqlInsertCols + sqlInsertRow;

                    InsertRow(insertStatement, sc);
                }
            }
        }

        private static void InsertRow(string insertStatement, SqlConnection sc)
        {
            try
            {
                SqlCommand insertData = new SqlCommand(insertStatement, sc);
                insertData.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //if there's an error, don't stop.  keep going with the next record.
                ReportError(ex.Message, ex.StackTrace);
            }
        }

        private static bool CreateTable(string sqlCreateTableScript)
        {
            string conn = ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;

            try
            {
                using (SqlConnection sc = new SqlConnection(conn))
                {
                    sc.Open();
                    SqlCommand createTable = new SqlCommand(sqlCreateTableScript, sc);
                    createTable.ExecuteNonQuery();
                    return true;
                }
            }
            catch (Exception ex)
            {
                ReportError(ex.Message, ex.StackTrace);
                return false;
            }
        }

        private static void ProcessTableData(List<string> fileContents, ref string tableName, ref List<string[]> rows, ref List<ColumnAttributes> colInfo)
        {

            //parse through the list of filecontents, getting the table name, columns and finally the data to insert

            int rowNumber = 0;

            foreach (string row in fileContents)
            {
                if (rowNumber == 0)
                {
                    tableName = row;
                }
                else
                {
                    if (rowNumber == 1)
                    {
                        colInfo = ParseColumns(row);
                    }
                    else
                    {
                        rows = ParseInsertRow(row, rows, ref colInfo);
                    }
                }
                rowNumber++;
            }

            //now that we have figured out the datatypes, let's go back and add single quotes around varchars and dates
            foreach (string[] r in rows)
            {
                for (int x = 0; x <= r.Length - 1; x++)
                {
                    ColumnAttributes colAtt = colInfo[x];
                    if (colAtt.DataType.Contains("nvarchar") || colAtt.DataType.Contains("datetime"))
                    {
                        r[x] = string.Format("'{0}'", r[x]);
                    }

                }
            }

        }

        private static string GenerateCreateTableScript(string tableName, List<ColumnAttributes> colInfo)
        {
            string sqlsc;
            sqlsc = string.Format("CREATE TABLE {0} (", tableName);
            foreach (ColumnAttributes col in colInfo)
            {
                sqlsc += string.Format("{0}[{1}] {2}", Environment.NewLine, col.Name, col.DataType);
                switch (col.DataType)
                {
                    case "nvarchar":
                        sqlsc += string.Format (" ({0}), ", col.Length);
                        break;
                    case "decimal":
                        sqlsc += string.Format (" ({0},{1}), ", col.Length, col.Precision);
                        break;
                    default:
                        sqlsc += ", ";
                        break;
                }
            }
            sqlsc = sqlsc.TrimEnd(',', ' ') + ");" + Environment.NewLine;

            return sqlsc;

        }

        private static List<string[]> ParseInsertRow(string row, List<string[]> insertRows, ref List<ColumnAttributes> colInfo)
        {
            List<string> fields = row.Split(',').ToList<string>();

            for (int x = 0; x <= fields.Count-1; x++)
            { 
                //check for date, int, or decimal.  if not one of those, call it nvarchar.
                //once it is nvarchar, it will never change.
                ColumnAttributes colAtt = colInfo[x];
                colAtt.UpdateColumnAttributes(fields[x]);

            }

            //put the row of fields into the list of rows to be inserted into the table.
            string[] fieldArray = fields.ToArray<string>();
            insertRows.Add(fieldArray);
            return insertRows;
        }

        private static List<ColumnAttributes> ParseColumns(string row)
        {

            //initialize the column information

            List<string> colNames = row.Split(',').ToList<string>();

            List<ColumnAttributes> colList = new List<ColumnAttributes>();

            foreach (string name in colNames)
            {
                ColumnAttributes ca = new ColumnAttributes(name);
                colList.Add(ca);
            }

            return colList;
        }

        private static bool DatabaseTablesExist()
        {
            string conn = ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;

            using (SqlConnection sc = new SqlConnection(conn))
            {
                string cmdText = @"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES) SELECT 1 ELSE SELECT 0";
                sc.Open();
                SqlCommand doExist = new SqlCommand(cmdText, sc);
                bool es = Convert.ToBoolean(doExist.ExecuteScalar());

                //wait for a while to let the database update before we try to insert anything;
                Thread.Sleep(200);
                return es;

            }
        }
    }
}
