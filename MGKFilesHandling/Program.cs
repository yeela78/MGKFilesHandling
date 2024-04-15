using OracleLayer;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Text.Json;
using System.Xml;

namespace MGKFilesHandling
{
    public class Program
    {
        public static string Parking_Connection = "Data Source=mpkcust; User ID=SVIVA;Password=Mpark0818;";
        //public static string Parking_Connection = "Data Source=custstd; User ID=SVIVA;Password=SVIVA;";

        public static string SafeLocationRead = @"\\172.30.23.50\eca$\From_MGK";
        //public static string SafeLocationRead = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles\From_MGK";

        public static string SafeLocationWrite = @"\\172.30.23.50\eca$\To_MGK";
        //public static string SafeLocationWrite = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles\To_MGK";

///*        
        public static string localFolder = @"\MGKFiles";
        public static string stageFolder = @"\MGKFiles\Stage";
        public static string archiveFolder = @"\MGKFiles\Archive";
        public static string errorFolder = @"\MGKFiles\Error";
        public static string toMGKFolder = @"\MGKFiles\To_MGK";
        public static string logPortionFolder = @"\\172.30.12.3\metropark.osb.logs$\Prod\MPFinesCollectionCenter\SendNewPortion";
        public static string logMashovFolder = @"\\172.30.12.3\metropark.osb.logs$\Prod\MPFinesCollectionCenter\ReceivePortionMashov";
/*
        public static string localFolder = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles";
        public static string stageFolder = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles\Stage";
        public static string archiveFolder = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles\Archive";
        public static string errorFolder = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles\Error";
        public static string archiveError = @"\\172.30.12.3\metropark.osb.resources$\sviva\2\SUPERVISION\MGKFiles\To_MGK";
        public static string logPortionFolder = @"\\172.30.12.3\metropark.osb.logs$\Dev\MPFinesCollectionCenter\SendNewPortion";
        public static string logMashovFolder = @"\\172.30.12.3\metropark.osb.logs$\Dev\MPFinesCollectionCenter\ReceivePortionMashov";
*/
        public static string handleMashovURL = "https://oramw.metropark.co.il/MPFinesCollectionCenter/ReceivePortionMashovNewRest";
        //public static string handleMashovURL = "https://oramwtest.metropark.co.il/MPFinesCollectionCenter/ReceivePortionMashovNewRest";
        public static string SendPortionURL = "https://oramw.metropark.co.il/MPFinesCollectionCenter/SendNewPacketRest";
        //public static string SendPortionURL = "https://oramwtest.metropark.co.il/MPFinesCollectionCenter/SendNewPacketRest";
        public static string ReceivedPortionURL = "https://oramwtest.metropark.co.il/MPFinesCollectionCenter/";


        private static void WriteLog(string logFolder, string strLog)
        {
            string currentDate = DateTime.Now.ToString("yyyyMMdd");
            string currentTime = DateTime.Now.ToString("HH:mm:ss");

            try
            {
                File.WriteAllText(logFolder + "\\" + currentDate + "_Info.txt", currentTime + ": " + strLog);
            }
            catch (Exception e) { }
        }

        public static string POST(string url, string jsonContent)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.Method = "POST";
            string responseStr = string.Empty;
            jsonContent = jsonContent.Replace("\\","\\\\");

            System.Text.UTF8Encoding encoding = new System.Text.UTF8Encoding();
            Byte[] byteArray = encoding.GetBytes(jsonContent);

            request.ContentLength = byteArray.Length;
            request.ContentType = @"application/json";

            using (Stream dataStream = request.GetRequestStream())
            {
                dataStream.Write(byteArray, 0, byteArray.Length);
            }
            long length = 0;
            try
            {
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    length = response.ContentLength;
                    if (length == 0)
                    {
                        responseStr = (response.StatusCode == HttpStatusCode.OK ? "OK" : response.StatusDescription);
                    }
                    else
                    {
                        Byte[] resByteArray = new Byte[length];
                        using (Stream resStream = response.GetResponseStream())
                        {
                            resStream.Read(resByteArray, 0, resByteArray.Length);
                            responseStr = resByteArray.ToString();
                            responseStr = (responseStr == "OK" ? "OK" : responseStr);
                        }
                    }
                }
            }
            catch (WebException ex)
            {
                // Log exception and throw as for GET example above
            }
            return responseStr;
        }


        static DataTable GetNewPortionToHandle ()
        {
            OracleDB dataAccess = null;
            DataTable dt = null;
            int i;
            try
            {
                dataAccess = new OracleDB(Parking_Connection);

                StringBuilder sb = new StringBuilder();

                sb.AppendFormat(" select ");
                sb.AppendFormat(" t.mana as MANA");
                sb.AppendFormat(" from ");
                sb.AppendFormat(" service.mgk_send_header t ");
                sb.AppendFormat(" where ");
//                sb.AppendFormat(" t.mun_Def =  {0} ", mun_def);
//                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.polling_status =  '0' ");
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.status_send =  1 ");
                sb.AppendFormat(" order by ");
                sb.AppendFormat(" t.mana desc ");

                dt = dataAccess.GetData(sb.ToString());
                if (dt.Rows.Count > 0)
                {
                    string mana = dt.Rows[0]["MANA"].ToString();
                    sb.Clear();
                    sb.AppendFormat(" update ");
                    sb.AppendFormat(" service.mgk_send_header t ");
                    sb.AppendFormat(" set t.polling_status =  '{0}' ", "Yossi");
                    sb.AppendFormat(" where ");
//                    sb.AppendFormat(" t.mun_Def =  {0} ", mun_def);
//                    sb.AppendFormat(" and ");
                    sb.AppendFormat(" t.mana = {0} ", mana);
                    sb.AppendFormat(" and ");
                    sb.AppendFormat(" t.status_send =  1 ");

                    i = dataAccess.ExecuteNonQuery(sb.ToString());
                }
                sb.Clear();
                sb.AppendFormat(" select ");
                sb.AppendFormat(" t.mun_def as MUN_DEF, t.system_id as SYSTEM_ID, t.mana as MANA");
                sb.AppendFormat(" from ");
                sb.AppendFormat(" service.mgk_send_header t ");
                sb.AppendFormat(" where ");
//                sb.AppendFormat(" t.mun_Def =  {0} ", mun_def);
//                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.polling_status =  'Yossi' ");
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.status_send =  1 ");
                sb.AppendFormat(" order by ");
                sb.AppendFormat(" t.mana ");

                dt = dataAccess.GetData(sb.ToString());

            }
            catch (Exception e)
            {
                throw e;
            }
            return dt;
        }

        static string UpdatePollingStatusForNewPortion(string mun_def, string mana_id)
        {
            OracleDB dataAccess = null;
            int i = 0;
            try
            {
                dataAccess = new OracleDB(Parking_Connection);

                StringBuilder sb = new StringBuilder();

                sb.AppendFormat(" update ");
                sb.AppendFormat(" service.mgk_send_header t ");
                sb.AppendFormat(" set t.polling_status =  '1' ");
                sb.AppendFormat(" where ");
                sb.AppendFormat(" t.mun_Def =  {0} ", mun_def);
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.mana = {0} ", mana_id);
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.polling_status = '{0}' ", "Yossi");
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.status_send =  2 ");

                i = dataAccess.ExecuteNonQuery(sb.ToString());
            }
            catch (Exception e)
            {
                throw e;
            }
            return i.ToString(); ;
        }

        static string GetMunDefBaseFolder(string mun_def, string system_id)
        {
            OracleDB dataAccess = null;
            DataTable dt = null;
            string schemaName = string.Empty;
            string basePathName = string.Empty;
            int i = 0;
            try
            {
                dataAccess = new OracleDB(Parking_Connection);

                StringBuilder sb = new StringBuilder();

                sb.AppendFormat(" select s.schema_name as SCHEMA_NAME ");
                sb.AppendFormat(" from service.osb_municipal_info t inner join service.osb_data_source_schema s ");
                sb.AppendFormat(" on t.data_source_name = s.data_source ");
                sb.AppendFormat(" where ");
                sb.AppendFormat(" t.mun_Def =  {0} ", mun_def);
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.system_id = {0} ", system_id);

                dt = dataAccess.GetData(sb.ToString());
                if (dt.Rows.Count ==  1)
                {
                    schemaName = dt.Rows[0]["SCHEMA_NAME"].ToString();
                }

                sb.Clear();
                sb.AppendFormat(" select t.p_value as P_VALUE ");
                sb.AppendFormat(" from {0}.tbl_param t " , schemaName);
                sb.AppendFormat(" where ");
                sb.AppendFormat(" t.mun_Def =  {0} ", mun_def);
                sb.AppendFormat(" and ");
                sb.AppendFormat(" t.param_id = 151");

                dt = dataAccess.GetData(sb.ToString());
                if (dt.Rows.Count == 1)
                {
                    basePathName = dt.Rows[0]["P_VALUE"].ToString();
                }


            }
            catch (Exception e)
            {
                throw e;
            }
            return basePathName;
        }

        static string GetBaseFolderByMana(string mana_id)
        {
            OracleDB dataAccess = null;
            DataTable dt = null;
            string schemaName = string.Empty, mundef = string.Empty, systemId = string.Empty;
            string basePathName = string.Empty;
            int i = 0;
            try
            {
                dataAccess = new OracleDB(Parking_Connection);

                StringBuilder sb = new StringBuilder();

                sb.AppendFormat(" select ");
                sb.AppendFormat(" t.mun_def as MUN_DEF, t.system_id as SYSTEM_ID");
                sb.AppendFormat(" from ");
                sb.AppendFormat(" service.mgk_send_header t ");
                sb.AppendFormat(" where ");
                sb.AppendFormat(" t.mana = {0} ", mana_id);

                dt = dataAccess.GetData(sb.ToString());
                if (dt.Rows.Count == 1)
                {
                    mundef = dt.Rows[0]["MUN_DEF"].ToString();
                    systemId = dt.Rows[0]["SYSTEM_ID"].ToString();

                    sb.Clear();
                    sb.AppendFormat(" select s.schema_name as SCHEMA_NAME ");
                    sb.AppendFormat(" from service.osb_municipal_info t inner join service.osb_data_source_schema s ");
                    sb.AppendFormat(" on t.data_source_name = s.data_source ");
                    sb.AppendFormat(" where ");
                    sb.AppendFormat(" t.mun_Def =  {0} ", mundef);
                    sb.AppendFormat(" and ");
                    sb.AppendFormat(" t.system_id = {0} ", systemId);

                    dt = dataAccess.GetData(sb.ToString());
                    if (dt.Rows.Count == 1)
                    {
                        schemaName = dt.Rows[0]["SCHEMA_NAME"].ToString();
                    }
                    sb.Clear();
                    sb.AppendFormat(" select t.p_value as P_VALUE ");
                    sb.AppendFormat(" from {0}.tbl_param t ", schemaName);
                    sb.AppendFormat(" where ");
                    sb.AppendFormat(" t.mun_Def =  {0} ", mundef);
                    sb.AppendFormat(" and ");
                    sb.AppendFormat(" t.param_id = 151");

                    dt = dataAccess.GetData(sb.ToString());
                    if (dt.Rows.Count == 1)
                    {
                        basePathName = dt.Rows[0]["P_VALUE"].ToString();
                    }

                }

            }
            catch (Exception e)
            {
                throw e;
            }
            return basePathName;
        }

        static void CheckForPortionMashovFiles(string[] args)
        {
            // Need to see if folder for files if found then start handle
            string response;
            string fileExc = string.Empty;
            string basePathName = string.Empty;
            string mana = string.Empty;
            string[] fields = null;
            try
            {
                string request = "{\"PP_FILE_NAME\":\"XX_FILENAME\", \"PP_PATH_NAME\":\"XX_PATHNAME\"}";
                DirectoryInfo di = new DirectoryInfo(SafeLocationRead);
                var regexTest = new Func<string, bool>(i => Regex.IsMatch(i, @"4920_to_mgk_\d+_\d+\.MASHOV\.xml", RegexOptions.Compiled | RegexOptions.IgnoreCase));
                FileInfo[] files = di.GetFiles("4920_to_mgk_*MASHOV*.xml").OrderBy(p => p.CreationTime).ToArray();

                foreach (FileInfo file in files)
                {
                    fileExc = file.Name;
                    fields = fileExc.Split('_');
                    mana = fields[3]; // for example: 4920_to_mgk_<mana>_<dd-mm-yyyy>_MASHOV.xml
                    basePathName = GetBaseFolderByMana(mana);
                    try
                    {
                        if (!Directory.Exists(basePathName + stageFolder))
                            Directory.CreateDirectory(basePathName + stageFolder);

                        file.MoveTo(basePathName + stageFolder + "\\" + file.Name);
                    } catch (Exception e) { File.Move(file.FullName, basePathName + stageFolder + "\\" + file.Name);}
                    
                    response = POST(handleMashovURL, request.Replace("XX_FILENAME", file.Name).Replace("XX_PATHNAME", basePathName + stageFolder));

                    if (response != null && response.Equals("OK"))
                    {
                        if (!Directory.Exists(basePathName + archiveFolder))
                            Directory.CreateDirectory(basePathName + archiveFolder);
                        File.Move(basePathName + stageFolder + "\\" + file.Name, basePathName + archiveFolder + "\\" + DateTime.Now.Ticks + "_" + file.Name);
                    }
                    else
                    {
                        WriteLog(logMashovFolder, file.Name + " - Error: " + response);
                        if (!Directory.Exists(basePathName + errorFolder))
                            Directory.CreateDirectory(basePathName + errorFolder);
                        File.Move(basePathName + stageFolder + "\\" + file.Name, basePathName + errorFolder + "\\" + DateTime.Now.Ticks + "_" + file.Name);
                    }
                }

            }
            catch (Exception e) {
                WriteLog(logMashovFolder, fileExc + " - Error: " + e.Message);
            }

        }

        static void CheckForNewPortionToSend(string[] args)
        {
            // Need to see if folder for files if found then start handle
            string currentRequest;
            string response = string.Empty;
            string mana = string.Empty;
            string mundef = string.Empty;
            string systemId = string.Empty;
            string basePathName = string.Empty;
            DataTable dt;
            string drExc = string.Empty;
            string fileToMgkName = "4920_to_mgk_<MANAID>_<DATE>.xml";
            try
            {
                string request = "{\"PP_MUN_DEF\": XX_MUNDEF, \"PP_MANA_ID\" : XX_MANAID}";

                dt = GetNewPortionToHandle();
                foreach (DataRow dr in dt.Rows)
                {
                    mundef = dr["MUN_DEF"].ToString();
                    systemId = dr["SYSTEM_ID"].ToString();
                    mana = dr["MANA"].ToString();
                    basePathName = GetMunDefBaseFolder(mundef, systemId);

                    if (!Directory.Exists(basePathName + toMGKFolder))
                        Directory.CreateDirectory(basePathName + toMGKFolder);

                    drExc = mundef + ", " + systemId + ", " + mana;
                    currentRequest = request.Replace("XX_MANAID", mana).Replace("XX_MUNDEF", mundef);
                    //response = POST(SendPortionURL, request);


                    var client = new RestClient(SendPortionURL);
                    client.Timeout = -1;
                    var requestIn = new RestRequest(Method.POST);
                    DateTime currDate = DateTime.Now;
                    requestIn.AddHeader("Content-Type", "application/json");
                    requestIn.AddJsonBody(currentRequest, "application/json");
                    //requestIn.AddParameter("application/json", currentRequest, ParameterType.RequestBody);
                    IRestResponse responseOut = client.Execute(requestIn);
                    Console.WriteLine(responseOut.Content);

                    if (responseOut!= null && responseOut.IsSuccessful)
                    {
                        fileToMgkName = fileToMgkName.Replace("<MANAID>", mana.PadLeft(6, '0'));
                        fileToMgkName = fileToMgkName.Replace("<DATE>", currDate.ToString("ddMMyyyy"));

                        File.Copy(basePathName + toMGKFolder + "\\" + fileToMgkName, SafeLocationWrite + "\\" + fileToMgkName);

                        string i = UpdatePollingStatusForNewPortion(mundef, mana);
                        WriteLog(logMashovFolder, mundef + ", " + mana + " - Response: " + responseOut);
                    }
                    else
                    {
                        WriteLog(logMashovFolder, mundef + ", " + mana + " - Error: " + responseOut);
                    }
                    return;
                }

            }
            catch (Exception e)
            {
                WriteLog(logMashovFolder, drExc + " - Error: " + e.Message);
            }

        }
        static void CheckForRecievedFiles(string[] args)
        {
            int mana = 0;
            if (!Directory.Exists(SafeLocationRead))
                Directory.CreateDirectory(SafeLocationRead);
            //DirectoryInfo di = new DirectoryInfo(SafeLocationRead);
            //FileInfo[] files = di.GetFiles("4920_from_mgk_*.xml").ToArray();
            string[] files = Directory.GetFiles(SafeLocationRead)
                                  .Where(file => Path.GetFileName(file).StartsWith("4920_from_mgk_"))
                                  .ToArray();
            if(files.Length > 0)
            {
                mana = GetCurrentReceivedMana();
                string fileFromMgkName = "4920_from_mgk_<MANAID>";
                fileFromMgkName = fileFromMgkName.Replace("<MANAID>", mana.ToString().PadLeft(6, '0'));
                bool fileExists = Directory.GetFiles(SafeLocationRead)
                                   .Any(file => Path.GetFileName(file).StartsWith(fileFromMgkName));
                if(fileExists)
                {
                    string filename = Directory.GetFiles(SafeLocationRead)
                                   .First(file => Path.GetFileName(file).StartsWith(fileFromMgkName));
                    XmlDocument xmlDoc = new XmlDocument();
                    xmlDoc.Load(filename);
                    XmlNode header = xmlDoc.SelectSingleNode("/updates_from_mgk/InterfaceHeader");
                    XmlNode list = xmlDoc.SelectSingleNode("/updates_from_mgk/Elements");
                    string attributeValue = header.ChildNodes[9].FirstChild.Value;
                    int numOfElements = Convert.ToInt32(attributeValue);
                    if(numOfElements == list.ChildNodes.Count)
                    {
                        string sendDatestr = header.ChildNodes[5].FirstChild.Value;
                        DateTime sendDate = ParseDate(sendDatestr);
                        //todo: insert to MGK_RECEIVE_HEADER - mana, sysCount, fileName, send_date, update_name
                        int result = InsertMgkReceivedHeader(mana, numOfElements, filename.Split('\\')[filename.Split('\\').Length - 1], sendDate);
                        foreach (XmlNode  node in list.ChildNodes)
                        {
                            string sys = node.ChildNodes[1].ChildNodes[1].FirstChild.Value;
                            string mgkNumber = node.ChildNodes[1].ChildNodes[0].FirstChild.Value;
                            int actionType = Convert.ToInt16(node.ChildNodes[0].ChildNodes[3].FirstChild.Value);
                            decimal sumPay = 0;
                            DateTime payDate = new DateTime();
                            DateTime closeDate = new DateTime();
                            if (actionType == 2)
                            {
                                sumPay = Convert.ToDecimal(node.ChildNodes[1].ChildNodes[8].ChildNodes[1].FirstChild.Value);
                                string dateStr = node.ChildNodes[1].ChildNodes[8].ChildNodes[2].FirstChild.Value;
                                payDate = ParseShortDate(dateStr);
                            }
                            if(actionType == 1)
                            {
                                closeDate = ParseShortDate(node.ChildNodes[1].ChildNodes[5].FirstChild.Value);
                            }
                            Result res = InsertMgkReceivedRow(mana, Convert.ToInt32(mgkNumber), actionType, sys, sumPay, payDate, closeDate);
                            //TODO: insert respone to mgkFile
                        }
                    }
                    else
                    {
                        //TODO: SEND Mashov to mgk with error code - num of elements wrong
                    }

                }
                else
                {
                    //TODO: SEND Mashov to mgk with error code - file not exist
                }


            }



        }
        static DateTime ParseDate(string dtString)
        {
            string[] arrTemp = dtString.Split(' ');
            string[] dateTemp = arrTemp[0].Split('/');
            string[] timeTemp = arrTemp[1].Split(':');

            DateTime dt = new DateTime(Convert.ToInt16(dateTemp[0]), Convert.ToInt16(dateTemp[1]), Convert.ToInt16(dateTemp[2]), Convert.ToInt16(timeTemp[0]), Convert.ToInt16(timeTemp[1]), Convert.ToInt16(timeTemp[2].Split('.')[0]));
            return dt;
        }
        static DateTime ParseShortDate(string dtString)
        {
            string[] dateTemp = dtString.Split('-');

            DateTime dt = new DateTime(Convert.ToInt16(dateTemp[0]), Convert.ToInt16(dateTemp[1]), Convert.ToInt16(dateTemp[2]));
            return dt;
        }
        static int InsertMgkReceivedHeader (int mana, int sysCount, string fileName, DateTime sendDate)
        {
            int result = 0;
            var client = new RestClient(ReceivedPortionURL + "InsertMgkReceivedHeaderRest");
            client.Timeout = -1;
            var requestIn = new RestRequest(Method.POST);
            DateTime currDate = DateTime.Now;
            requestIn.AddHeader("Content-Type", "application/json");
            requestIn.AddJsonBody("{ \"PP_MANA\":"+mana.ToString()+",\"PP_SYS_COUNT\":"+sysCount.ToString()+",\"PP_FILE_NAME\" : \""+fileName+"\",\"PP_SEND_DATE\" : \""+sendDate.ToString("o") + "\", \"PP_UPDATE_NAME\" : \"OSB_WS\"}", "application/json");
            //requestIn.AddParameter("application/json", currentRequest, ParameterType.RequestBody);
            IRestResponse responseOut = client.Execute(requestIn);
            if (responseOut != null && responseOut.Content != null && responseOut.StatusDescription == "OK")
            {
                JsonDocument res = JsonDocument.Parse(responseOut.Content);

                // Accessing JSON properties
                JsonElement root = res.RootElement;
                return root.GetProperty("PP_OUT_RESULT").GetInt32();
            }
            else
                return 0;
           
        }
        static Result InsertMgkReceivedRow(int mana, int mgkNumber, int actionType, string sys, decimal sumPay, DateTime payDate, DateTime closeDate)
        {
            Result res = new Result();
            var client = new RestClient(ReceivedPortionURL + "InsertMGKReceiveRowRest");
            client.Timeout = -1;
            var requestIn = new RestRequest(Method.POST);
            DateTime currDate = DateTime.Now;
            requestIn.AddHeader("Content-Type", "application/json");
            string body = "{\"PP_MANA\" : " + mana.ToString() + ", \"PP_SYS\" : " + sys + ", \"PP_MGKNUMBER\" : " + mgkNumber.ToString() + ", \"PP_ACTIONTYPE\" : " + actionType + ", \"PP_SUMPAY\" : " + sumPay.ToString() + ", \"PP_PAYDATE\" : \"" + payDate.ToString("o") + "\", \"PP_CLOSEDDATE\" : \"" +closeDate.ToString("o") + "\", \"PP_UPDATE_NAME\" : \"OSB_WS\"}";
            requestIn.AddJsonBody(body, "application/json");
            //requestIn.AddParameter("application/json", currentRequest, ParameterType.RequestBody);
            IRestResponse responseOut = client.Execute(requestIn);
            if (responseOut != null && responseOut.Content != null && responseOut.StatusDescription == "OK")
            {
                JsonDocument resOut = JsonDocument.Parse(responseOut.Content);

                // Accessing JSON properties
                JsonElement root = resOut.RootElement;
                res.resultCode = root.GetProperty("PP_OUT_RESULT").GetInt32();
                res.resultDesc = root.GetProperty("PP_OUT_ERRBUF").GetString();
                return res;
            }
            else
                return res;      
        }
        static int GetCurrentReceivedMana()
        {
           // int mana;
            var client = new RestClient(ReceivedPortionURL + "GetCurrentReceivedManaRest");
            client.Timeout = -1;
            var requestIn = new RestRequest(Method.POST);
            DateTime currDate = DateTime.Now;
            requestIn.AddHeader("Content-Type", "application/json");
            requestIn.AddJsonBody("{}", "application/json");
            //requestIn.AddParameter("application/json", currentRequest, ParameterType.RequestBody);
            IRestResponse responseOut = client.Execute(requestIn);
            if (responseOut != null && responseOut.Content != null && responseOut.StatusDescription == "OK")
            {
                JsonDocument res = JsonDocument.Parse(responseOut.Content);

                // Accessing JSON properties
                JsonElement root = res.RootElement;
                return root.GetProperty("GETCURRENTRECEIVEDMANA").GetInt32();// responseOut.Content;
            }
            else
                return 0;
        }
        static void Main(string[] args)
        {
            //ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

            //CheckForPortionMashovFiles(args);
            //CheckForNewPortionToSend(args);
            CheckForRecievedFiles(args);
        }
    }
}

public class Result
{
    public int resultCode { get; set; }
    public string resultDesc { get; set; }
}
