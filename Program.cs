using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Globalization;
using System.Text.RegularExpressions;
using System.IO.Compression;
using System.Configuration;
using System.Collections.Specialized;
using System.Data;

namespace SqlToCsv
{
    class Program
    {
        static bool zip = false;

        static string quoteChar = "";                 // quote character for CSV - if set, escapeChar is ignored
        static string escapeChar = @"\";                // escape character for TEXT - set quote to empty first
        static string dropRegex = ""; //@"[\x00]";      // characters matched by this regex are dropped (eg. \x00)
        static string escapeRegex = ""; //@"[\x00-\x19]";     // characters/strings matched by this regex are escaped with \xXX sequence
        static string delimiter = System.Globalization.CultureInfo.CurrentCulture.TextInfo.ListSeparator;                  // field delimiter
        static string newline = "\r\n";                   // row delimiter
        static string nullString = "";                  // DBNull is represented by this string

        static string serverName = ".";
        static string databaseName = "master";
        static int commandTimeout = 1800;

        static string sqlStatement = "";    // EXEC dwh.FetchMessageLog @readerId = $(@readerId), @columns = '$(@columns)'"; //"SELECT top 10 * FROM archiving.ArchiveTables"; 
        static string keyColumns = "";      //"SendDate, DatabaseId, TableId"; // "CreatedAt, DatabaseId, Id";
        static string ignoreColumns = "";   // comma delimited list of columns that, if returned by query, will not be included in output file(s)

        static string fileTemplate = "output_{0}.csv";
        static long maxRecordsPerFile = long.MaxValue;
        static bool doManifest = true;

        /*********************************************************************************************************************************************************************/

        //static string escapeRegexEx;

        static bool echo = false;

        static Dictionary<string, string> tempFileNames = new Dictionary<string, string>();
        static Dictionary<string, StreamWriter> writers = new Dictionary<string, StreamWriter>();
        static Dictionary<string, GZipStream> zipstreams = new Dictionary<string, GZipStream>();
        static Dictionary<string, FileStream> filestreams = new Dictionary<string, FileStream>();
        static Dictionary<string, long> rowsInFiles = new Dictionary<string, long>();                     // rows in file
        static Dictionary<string, int> fileNumbers = new Dictionary<string, int>();                     // file number

        static StringBuilder lineBuilder = new StringBuilder();
        static StringBuilder header = new StringBuilder();
        static StringBuilder manifest = new StringBuilder();

        static NameValueCollection sqlParams1 = ((NameValueCollection)ConfigurationManager.GetSection("sqlParams"));
        static NameValueCollection sqlParams2 = new NameValueCollection(); // fill from cmd line params

        static bool interactive = false;
        static int runs = 1;

        static string[] keyCols;
        static string[] ignorCols;
        private static Regex escapeRgx;
        static Regex rgxAnyChar = new Regex(".", RegexOptions.Singleline | RegexOptions.Compiled);
        static string rplcEscape;

        static void writeHelp()
        {
            Console.WriteLine("Supported parameters:" +
            "\n" +
            "\n    -s server                           Server name." +
            "\n" +
            "\n    -d database                         Database name." +
            "\n" +
            "\n    -q \"SELECT * FROM dbo.Table;\"     SQL query to execute." +
            "\n" +
            "\n    -f pathPattern                      File path/name pattern. Uses .NET format syntax. " +
            "\n                                        Param {0} is replaced with file row count." + 
            "\n                                        Other params are values from key columns." +
            "\n" +
            "\n    -n newline                          New line separator. Default is \\r\\n." +
            "\n" +
            "\n    -l columnSeparator                  Column separator. Default is '" + delimiter + "'." +
            "\n" +
            "\n    -k \"list, of, key, columns\"         Comma delimited list of key kolumns. Optional parameter." +
            "\n                                        If key columns are specified, values in those columns are used to split output in more than one file." +
            "\n                                        All the records with identical combination of values are placed in one file." +
            "\n                                        If used, path pattern should include placeholders for every key column. " +
            "\n                                        Otherwise, files will overwrite each other." +
            "\n                                        E.g. -k \"SendDate, DatabaseId\" -f \"c:\\files\\file_{1:yyyyMMdd}_{2}.csv\"" +
            "\n" +
            "\n    -g \"list, of, columns\"              Columns returned from query that are to be ignored when outputing to file." +
            "\n                                        Those columns can still be used as key columns for file creation." +
            "\n" +
            "\n    -z                                  GZip output files. Every file is compressed separately." + 
            "\n                                        ZIP extension is added to specified file name." +
            "\n" +
            "\n    -x #                                Maximum number of records in one file. " +
            "\n                                        If excedeed, _XXX sufix is added to file name and new file is generated." +
            "\n                                        Can be combined with key columns." +
            "\n" +
            "\n    -t #                                SQL command timeout in seconds. Default is 1800 (30 min)." +
            "\n" +
            ""
            );


        }

        static void Main(string[] args)
        {
            #region settings

            zip = Settings.Default.Zip;
            if (!String.IsNullOrEmpty(Settings.Default.QuoteChar)) quoteChar = Regex.Unescape(Settings.Default.QuoteChar);
            if (!String.IsNullOrEmpty(Settings.Default.EscapeChar)) escapeChar = Regex.Unescape(Settings.Default.EscapeChar);
            if (!String.IsNullOrEmpty(Settings.Default.DropRegex)) dropRegex = Settings.Default.DropRegex;
            if (!String.IsNullOrEmpty(Settings.Default.EscapeRegex)) escapeRegex = Settings.Default.EscapeRegex;
            if (!String.IsNullOrEmpty(Settings.Default.Delimiter)) delimiter = Regex.Unescape(Settings.Default.Delimiter);
            if (!String.IsNullOrEmpty(Settings.Default.NewLine)) newline = Regex.Unescape(Settings.Default.NewLine);
            if (!String.IsNullOrEmpty(Settings.Default.NullString)) nullString = Regex.Unescape(Settings.Default.NullString);
            if (!String.IsNullOrEmpty(Settings.Default.ServerName)) serverName = Regex.Unescape(Settings.Default.ServerName);
            if (!String.IsNullOrEmpty(Settings.Default.DatabaseName)) databaseName = Regex.Unescape(Settings.Default.DatabaseName);
            commandTimeout = Settings.Default.CommandTimeout;
            if (!String.IsNullOrEmpty(Settings.Default.SqlStatement)) sqlStatement = Regex.Unescape(Settings.Default.SqlStatement);
            if (!String.IsNullOrEmpty(Settings.Default.KeyColumns)) keyColumns = Regex.Unescape(Settings.Default.KeyColumns);
            if (!String.IsNullOrEmpty(Settings.Default.FileTemplate)) fileTemplate = Regex.Unescape(Settings.Default.FileTemplate);
            doManifest = Settings.Default.DoManifest;

            #endregion

            try
            {

                #region param parsing

                {
                    int i = 0;
                    while (i < args.Length)
                    {
                        string a = args[i];

                        // sql replacement params
                        if (a.StartsWith("@"))
                        {
                            i++;
                            if (i >= args.Length) throw new ParamParsingException(String.Format("SQL parameter missing a value: {0}", a));

                            sqlParams2[a] = args[i];
                        }
                        // switches
                        if (a.StartsWith("-") || a.StartsWith("/"))
                        {
                            switch (a.Substring(1, a.Length - 1))
                            {
                                // serverName
                                case "s":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    serverName = args[i];
                                    break;
                                // databaseName
                                case "d":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    databaseName = args[i];
                                    break;
                                // sqlStatement
                                case "q":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    sqlStatement = args[i];
                                    break;
                                // fileTemplate
                                case "f":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    fileTemplate = args[i];
                                    break;
                                // newline
                                case "n":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    newline = args[i];
                                    break;
                                // delimiter
                                case "l":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    delimiter = args[i];
                                    break;
                                // keyColumns
                                case "k":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    keyColumns = args[i];
                                    break;
                                // ignoreColumns
                                case "g":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    ignoreColumns = args[i];
                                    break;
                                // zip
                                case "z":
                                    zip = true;
                                    break;
                                // !zip
                                case "Z":
                                    zip = false;
                                    break;
                                // maX records per file
                                case "x":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    if (!long.TryParse(args[i], out maxRecordsPerFile))
                                        throw new ParamParsingException(string.Format("Cmdline param '{0}' value of wrong type: {1}", a, args[i]));
                                    break;
                                // commandTimeout
                                case "t":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    if (!int.TryParse(args[i], out commandTimeout))
                                        throw new ParamParsingException(string.Format("Cmdline param '{0}' value of wrong type: {1}", a, args[i]));
                                    break;

                                // interactive
                                case "i":
                                    interactive = true;
                                    break;
                                // manifest
                                case "m":
                                    doManifest = true;
                                    break;
                                // !manifest
                                case "M":
                                    doManifest = false;
                                    break;
                                // runs
                                case "r":
                                    i++;
                                    if (i >= args.Length)
                                        throw new ParamParsingException(string.Format("Value missing for cmdline param '{0}'", a));
                                    if (!int.TryParse(args[i], out runs))
                                        throw new ParamParsingException(string.Format("Cmdline param '{0}' value of wrong type: {1}", a, args[i]));
                                    break;
                                default:
                                    throw new ParamParsingException(String.Format("Unknown cmdline param {0}", a));
                            }
                        }

                        i++;
                    }
                }
                #endregion

                if (keyColumns.Trim() != "")
                {
                    keyCols = keyColumns.Split(',');
                    for (int i = 0; i < keyCols.Length; i++)
                        keyCols[i] = keyCols[i].Trim();
                }
                else
                {
                    keyCols = new string[0];
                }

                if (ignoreColumns.Trim() != "")
                {
                    ignorCols = ignoreColumns.Split(',');
                    for (int i = 0; i < keyCols.Length; i++)
                        keyCols[i] = keyCols[i].Trim();
                }
                else
                {
                    ignorCols = new string[0];
                }

                Console.Title = String.Format("[{0}].[{1}]", serverName, databaseName);
                //Console.WindowWidth = 100;
                Console.OutputEncoding = Encoding.UTF8;

                //Regex rgxAll = new Regex(".", RegexOptions.Singleline | RegexOptions.Compiled);
                //escapeRegexEx = escapeRegex + (escapeRegex == "" ? "" : "|") + Regex.Escape(escapeChar) + "|" + Regex.Escape(delimiter) + "|" + Regex.Escape(newline) + @"|\A" + Regex.Escape(nullString) + @"\z";
                escapeRgx = new Regex(escapeRegex + (escapeRegex == "" ? "" : "|") + Regex.Escape(escapeChar) + "|" + Regex.Escape(delimiter) + "|" + Regex.Escape(newline) + @"|\A" + Regex.Escape(nullString) + @"\z", RegexOptions.Singleline | RegexOptions.Compiled);
                rplcEscape = escapeChar + "$0";

                //Console.WriteLine(escapeRegexEx);


                #region sql statement params resolving

                string sql = sqlStatement;
                // fill params from cmdline - this goes first to override settings file
                sql = Regex.Replace(sql, @"\$\(([^\)]+)\)", new MatchEvaluator(m =>
                {
                    var key = m.Groups[1].Value;
                    Console.WriteLine(String.Format("{0}={1}", key, sqlParams2[key]));
                    return sqlParams2[key] ?? m.ToString();
                }));

                // fill params from settings
                sql = Regex.Replace(sql, @"\$\(([^\)]+)\)", new MatchEvaluator(m =>
                {
                    var key = m.Groups[1].Value;
                    Console.WriteLine(String.Format("{0}={1}", key, sqlParams1[key]));
                    return sqlParams1[key] ?? m.ToString();
                }));

                {
                    Match m = Regex.Match(sql, @"\$\(([^\)]+)\)");
                    if (m.Success)
                    {
                        throw new ParamParsingException(string.Format("Missing sql parameter {0} in expression {1}", m.Groups[1].Value, sql));
                    }
                }
                //Console.WriteLine(sql);


                #endregion


                if (String.IsNullOrEmpty(sqlStatement.Trim()))
                    throw new ParamParsingException("Missing required parameter \"-q\".");

                for (int r = 0; r < runs; r++)
                    doBiznjis(sql);

            }
            catch (ParamParsingException ppe)
            {
                Console.Error.WriteLine(ppe.Message);
                writeHelp();
            }

            if (interactive)
            {
                Console.WriteLine("Press ENTER to exit...");
                Console.ReadLine();
            }
        }

        static void doBiznjis(string sql)
        {
            int[] keyIdxs = new int[keyCols.Length];

            long rc = 0;
            long _rc = 0;
            DateTime ts0, ts1, ts2, ts3, _ts2;

            ts0 = DateTime.Now;

            header.Clear();
            manifest.Clear();

            string oldkey = ""; // used to detect key change to update console title

            using (SqlConnection conn = new SqlConnection(@"Data Source=$serverName;Initial Catalog=$databaseName;Integrated Security=SSPI;Packet Size=32767".Replace("$serverName", serverName).Replace("$databaseName", databaseName)))
            {
                conn.Open();


                using (SqlCommand cmd = new SqlCommand(sql, conn) { CommandTimeout = commandTimeout })
                {
                    ts1 = DateTime.Now;
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {

                        if (reader.HasRows)
                        {
                            try
                            {
                                var schema = reader.GetSchemaTable();

                                string[] sqlNames = new string[schema.Rows.Count];
                                string[] sqlTypes = new string[schema.Rows.Count];
                                int[] sqlLengths = new int[schema.Rows.Count];
                                int[] sqlScales = new int[schema.Rows.Count];
                                int[] sqlPrecisions = new int[schema.Rows.Count];
                                bool[] outputColumn = new bool[schema.Rows.Count];
                                SqlDbType[] sqlProviderType = new SqlDbType[schema.Rows.Count];
                                for (int i = 0; i < schema.Rows.Count; i++)
                                {
                                    sqlNames[i] = schema.Rows[i]["ColumnName"].ToString();
                                    sqlTypes[i] = schema.Rows[i]["DataTypeName"].ToString();
                                    sqlLengths[i] = int.Parse(schema.Rows[i]["ColumnSize"].ToString());
                                    sqlScales[i] = int.Parse(schema.Rows[i]["NumericScale"].ToString());
                                    sqlPrecisions[i] = int.Parse(schema.Rows[i]["NumericPrecision"].ToString());

                                    sqlProviderType[i] = (SqlDbType)schema.Rows[i]["NonVersionedProviderType"];

                                    outputColumn[i] = Array.IndexOf(ignorCols, sqlNames[i]) == -1;
                                }

                                #region construct header row
                                // reset key column indexes
                                for (int c = 0; c < keyIdxs.Length; c++)
                                    keyIdxs[c] = -1;

                                for (int idx = 0; idx < sqlNames.Length; idx++)
                                {
                                    string n = sqlNames[idx];

                                    for (int c = 0; c < keyCols.Length; c++)
                                    {
                                        if (n.Equals(keyCols[c], StringComparison.InvariantCultureIgnoreCase))
                                            keyIdxs[c] = idx;
                                    }

                                    if (outputColumn[idx])
                                        writeQuoted(n, header);
                                }
                                //if (useEOR)
                                //    writeQuoted("__eor__", header);

                                // check if ALL key columns are found in resultset
                                for (int c = 0; c < keyIdxs.Length; c++)
                                    if (keyIdxs[c] == -1)
                                        throw new Exception(String.Format("Key column [{0}] is not a part of resultset.", keyCols[c]));

                                #endregion

                                #region counstruct json manifest
                                if (doManifest)
                                {
                                    manifest.Append("{\n\"columns\":[");
                                    for (int i = 0; i < sqlNames.Length; i++)
                                    {
                                        if (outputColumn[i])
                                        {
                                            manifest.Append(i == 0 ? "\n" : "\n,");
                                            manifest.Append("{\"name\":\"");
                                            manifest.Append(sqlNames[i].Replace("\"", "\\\""));

                                            manifest.Append("\",\"type\":\"");
                                            manifest.Append(sqlTypes[i].Replace("\"", "\\\""));
                                            manifest.Append("\"");


                                            switch (sqlProviderType[i])
                                            {
                                                case SqlDbType.Char:
                                                case SqlDbType.VarChar:
                                                case SqlDbType.NChar:
                                                case SqlDbType.NVarChar:
                                                case SqlDbType.Text:
                                                case SqlDbType.NText:
                                                    manifest.Append(",\"length\":");
                                                    manifest.Append(sqlLengths[i]);
                                                    break;
                                                case SqlDbType.Decimal:
                                                case SqlDbType.Real:
                                                    manifest.Append(",\"scale\":");
                                                    manifest.Append(sqlScales[i]);

                                                    manifest.Append(",\"precision\":");
                                                    manifest.Append(sqlPrecisions[i]);
                                                    break;
                                            }
                                            manifest.Append("}");
                                        }
                                    }
                                    manifest.Append("\n]");
                                    manifest.Append("\n,\"key\":\"" + header.ToString().Replace("\"", "\\\"") + "\"");
                                    manifest.Append("\n}");
                                }
                                #endregion

                                #region process data
                                ts2 = DateTime.Now;
                                _ts2 = ts2;
                                while (reader.Read())
                                {
                                    rc++;

                                    object[] keyValues = new object[keyCols.Length + 1]; // idx 0 je rezerviran za nešto...
                                    for (int i = 0; i < keyValues.Length; i++)
                                        keyValues[i] = "{" + i.ToString() + "}";  // da ne izgubimo placeholdere u prvom replaceu

                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        int keyIdx = Array.IndexOf<int>(keyIdxs, i);

                                        if (reader.IsDBNull(i))
                                        {
                                            write(nullString);
                                            if (keyIdx != -1) keyValues[keyIdx + 1] = null;
                                        }
                                        else
                                        {
                                            object v = reader[i];

                                            if (keyIdx != -1) keyValues[keyIdx + 1] = v;

                                            if (outputColumn[i])
                                            {
                                                switch (reader.GetFieldType(i).Name)
                                                {
                                                    case "TimeSpan":
                                                        writeQuoted(((TimeSpan)v).ToString(@"hh\:mm\:ss\.ffffff"));
                                                        break;
                                                    case "DateTime":
                                                        switch (sqlTypes[i])
                                                        {
                                                            case "date":
                                                                writeQuoted(((DateTime)v).ToString("yyyy-MM-dd"));
                                                                break;
                                                            case "time":
                                                                writeQuoted(((DateTime)v).ToString("HH:mm:ss.fff"));
                                                                break;
                                                            default:
                                                                writeQuoted(((DateTime)v).ToString("yyyy-MM-dd HH:mm:ss.fff"));
                                                                break;
                                                        }
                                                        break;
                                                    case "String":
                                                        writeQuoted((string)v);  //u25AF = ▯
                                                        break;
                                                    case "Decimal":
                                                        write(((decimal)v).ToString(CultureInfo.InvariantCulture));
                                                        break;
                                                    case "Byte[]":
                                                        write(Convert.ToBase64String((byte[])v));
                                                        break;
                                                    default:
                                                        write(String.Format(CultureInfo.InvariantCulture, "{0}", v));
                                                        break;
                                                }
                                            }
                                        }
                                    }
                                    //if (useEOR)
                                    //    write("0");

                                    string key = Path.GetFullPath(String.Format(fileTemplate + (zip ? ".zip" : ""), keyValues));

                                    if (!fileNumbers.ContainsKey(key))
                                        fileNumbers[key] = 1;

                                    if (rowsInFiles.ContainsKey(key) && rowsInFiles[key] == maxRecordsPerFile)
                                    {
                                        close(key);
                                        rename(key, true);
                                        fileNumbers[key]++;
                                    }

                                    if (oldkey != key)
                                    {
                                        Console.Title = String.Format("[{0}].[{1}] => {2}", serverName, databaseName, key);
                                        oldkey = key;
                                    }

                                    if (!tempFileNames.ContainsKey(key))
                                    {
                                        tempFileNames[key] = Path.Combine(Path.GetDirectoryName(key), Path.GetFileName(key) + ".temp");
                                        rowsInFiles[key] = 0;
                                    }

                                    donewline(key);

                                    ts3 = DateTime.Now;
                                    if ((ts3 - _ts2).TotalMilliseconds >= 1000)
                                    {
                                        Console.WriteLine("{0}\t{1}\t{2}\t{3} 1/s", ts3 - ts2, rc, rc - _rc, rc / (ts3 - ts2).TotalSeconds);
                                        _ts2 = ts3;
                                        _rc = rc;
                                    }
                                }
                                ts3 = DateTime.Now;
                                Console.WriteLine("{0}\t{1}\t{2} 1/s", rc, ts3 - ts2, rc / (ts3 - ts2).TotalSeconds);
                                #endregion 
                            }
                            finally
                            {
                                while (tempFileNames.Count > 0)
                                {
                                    string key = tempFileNames.Keys.First();
                                    close(key);
                                    rename(key, false);
                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("No rows found.");
                        }
                    }
                }
            }
        }

        static string quote(string value)
        {
            // first drop evil characters
            string val = Regex.Replace(value, dropRegex, "");

            // then encode acceptable content
            if (quoteChar != "")
                val = quoteChar + val.Replace(quoteChar, quoteChar + quoteChar) + quoteChar;
            else if (escapeChar != "")
                val = escape(val);

            return val;
        }


        static void writeQuoted(string value)
        {
            writeQuoted(value, lineBuilder);
        }

        static void writeQuoted(string value, StringBuilder builder)
        {
            write(quote(value), builder);
        }

        static void write(string value, StringBuilder builder)
        {
            if (builder.Length != 0)
                append(delimiter, builder);
            append(value, builder);
        }

        static void write(string value)
        {
            write(value, lineBuilder);
        }


        static string hexify(string value, Regex rgx)
        {
            // 0x01
            // \x00001

            return rgx.Replace(value, new MatchEvaluator((Match match) =>
            {
                return rgxAnyChar.Replace(match.Value, rplcEscape);
            }));

            /*
            return rgx.Replace(value, new MatchEvaluator((Match match) =>
            {
                if (match.Value.Length == 1)
                    return string.Format(@"{0}x{1:X2}", escapeChar, (int)match.Value.First());
                else
                    return string.Join("", match.Value.Select<char, string>((char c) => string.Format(@"{0}x{1:X2}", escapeChar, (int)c)));
            }));
            */
        }

        static string escape(string value)
        {
            return hexify(value, escapeRgx);
        }

        static void donewline(string key)
        {
            append(newline, lineBuilder);
            flush(key);
            rowsInFiles[key] = rowsInFiles[key] + 1;
        }

        static void append(string value, StringBuilder builder)
        {
            builder.Append(value);

            if (echo)
                Console.Write(value);
        }

        private static void open(string key)
        {
            if (!filestreams.ContainsKey(key))
            {
                ensureFolder(Path.GetDirectoryName(tempFileNames[key]));
                filestreams[key] = new FileStream(tempFileNames[key], FileMode.Create, FileAccess.Write, FileShare.None);
                if (writers.ContainsKey(key)) { writers[key].Close(); writers[key].Dispose(); writers.Remove(key); }
                if (zipstreams.ContainsKey(key)) { zipstreams[key].Close(); zipstreams[key].Dispose(); zipstreams.Remove(key); }
            }
            if (zip && !zipstreams.ContainsKey(key))
            {
                zipstreams[key] = new GZipStream(filestreams[key], CompressionLevel.Optimal);
            }
            if (!writers.ContainsKey(key))
            {
                writers[key] = new StreamWriter(zip ? (Stream)zipstreams[key] : (Stream)filestreams[key], Encoding.UTF8);
                writers[key].Write(header.ToString());
                writers[key].Write(newline);
            }
        }

        private static void close(string key)
        {
            if (writers.ContainsKey(key))
            {
                writers[key].Flush();
                writers[key].Close();
                writers[key].Dispose();
                writers.Remove(key);
            }

            if (zipstreams.ContainsKey(key))
            {
                zipstreams[key].Close();
                zipstreams[key].Dispose();
                zipstreams.Remove(key);
            }

            if (filestreams.ContainsKey(key))
            {
                filestreams[key].Close();
                filestreams[key].Dispose();
                filestreams.Remove(key);
            }
        }

        private static void flush(string key)
        {
            //Console.Write(lineBuilder.ToString());
            if (lineBuilder != null && lineBuilder.Length > 0)
            {
                open(key);
                writers[key].Write(lineBuilder.ToString());
            }
            lineBuilder = new StringBuilder();
        }

        private static void rename(string key, bool forceFileNumber)
        {
            if (File.Exists(tempFileNames[key]))
            {
                string p = Path.GetDirectoryName(key);
                string n = Path.GetFileNameWithoutExtension(key).TrimEnd('.');
                string e = Path.GetExtension(key);
                string fn = forceFileNumber || fileNumbers[key] > 1 ? String.Format("_{0:#000}", fileNumbers[key]) : "";
                while (n != Path.GetFileNameWithoutExtension(n))
                {
                    e = Path.GetExtension(n) + e;
                    n = Path.GetFileNameWithoutExtension(n);
                }

                string newFileName = Path.Combine(p, n + fn + e);
                newFileName = String.Format(newFileName, rowsInFiles[key]);

                if (File.Exists(newFileName))
                    File.Delete(newFileName);

                File.Move(tempFileNames[key], newFileName);

                tempFileNames.Remove(key);
                rowsInFiles.Remove(key);

                if (doManifest)
                {
                    File.WriteAllText(newFileName + ".json", manifest.ToString());
                }
            }
        }


        private static string ensureFolder(string name)
        {
            if (!Directory.Exists(name))
            {
                Console.WriteLine("Creating folder: {0}", name);
                Directory.CreateDirectory(name);
            }
            return name;
        }

    }

    public class ParamParsingException : Exception
    {
        public ParamParsingException()
        {
        }

        public ParamParsingException(string message)
        : base(message)
        {
        }

        public ParamParsingException(string message, Exception inner)
        : base(message, inner)
        {
        }
    }

}
