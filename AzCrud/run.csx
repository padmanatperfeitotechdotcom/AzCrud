#r "Newtonsoft.Json"

using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Net.Http;
using System.Net;
using System.Text;

public static HttpResponseMessage Run(HttpRequestMessage req, string tableName, int? id, TraceWriter log)
{
    var sql = "";
    var method = req.Method.ToString();

    switch (method) {
        case "GET":
            log.Info("SELECT");
            sql = Processor.Get(tableName, id);
            break;
        case "POST":
            log.Info("INSERT");
            sql = Processor.Add(tableName, req.Content);
            break;
        // case "PUT":
        // case "PATCH":
        //     log.Info("UPDATE");
        //     break;
        // case "DELETE":
        //     log.Info("PSOT");
        //     break;
        default:
            return req.CreateResponse(HttpStatusCode.NotImplemented);
    }

    var connString = ConfigurationManager.ConnectionStrings["db"].ConnectionString;
    if (method == "GET")
    {
        var dt = Processor.ExecuteSqlSelect(connString, sql);
        if (dt != null) {
            var json = "";
            var serializerSettings = new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
            json = JsonConvert.SerializeObject(dt, serializerSettings);
            return new HttpResponseMessage(HttpStatusCode.OK){ 
                Content = new StringContent(json, Encoding.UTF8, "application/json") 
                };
        } else {
            return req.CreateResponse(HttpStatusCode.InternalServerError);
        }
    }

    return Processor.ExecuteSqlCommand(connString, sql) ?
                req.CreateResponse(HttpStatusCode.OK) :
                req.CreateResponse(HttpStatusCode.InternalServerError);
}

public static class Processor {

    static public bool ExecuteSqlCommand(string connString, string sql)
    {
        var success = true;
        try
        {
            using (var connection = new SqlConnection(connString))
            {
                var cmd = new SqlCommand();
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                cmd.Connection = connection;
                connection.Open();
                cmd.ExecuteNonQuery();
            }
        }
        catch
        {
            success = false;
        }
        return success;
    }
    static public DataTable ExecuteSqlSelect(string connString, string sql)
    {
        var dataTable = new DataTable();
        try
        {
            using (var connection = new SqlConnection(connString))
            {
                var cmd = new SqlCommand();

                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                cmd.Connection = connection;
                connection.Open();

                var reader = cmd.ExecuteReader();
                dataTable.Load(reader);
            }
        }
        catch
        {
            dataTable = null;
        }
        return dataTable;
    }

    public static string Get(string tableName, int? id) {
        var sql = $"SELECT * FROM [{tableName}]";
        if (id.HasValue){
            sql = $"{sql} WHERE [Id] = {id}";
        }
        return sql;
    }

    public static JArray ParseJsonFromContent(HttpContent content){
        string contentString = content.ReadAsStringAsync().Result;
        contentString = JsonTidyAndConvertToArray(contentString);       
        return JArray.Parse(contentString);
    }

    static private string JsonTidyAndConvertToArray(string jsonString)
    {
        jsonString = jsonString.Trim();
        if (jsonString.Substring(0, 1) != "[")
        {
            jsonString = $"[{jsonString}]";
        }
        return jsonString;
    }

    public static string GetDbPropertyValue(JToken dbValue){
        switch (dbValue.Type){
            case JTokenType.Integer:
            case JTokenType.Float:
                return dbValue.ToString();
            default:
                return $"'{dbValue.ToString()}'";
        }
    }

    public static string Add(string tableName, HttpContent content) {
        var jArray = ParseJsonFromContent(content);
        var sql = "";

        foreach (var jToken in jArray)
        {
            sql += GenerateAddSql(jToken, tableName);
        }

        return sql;
    }

    static private string GenerateAddSql(JToken jToken, string tableName)
    {
        List<string> keyList = GetPropertyKeysForDynamic(jToken);
        var sqlKeys = "";
        var sqlValues = "";
        keyList.ForEach(key => {
            if (sqlKeys.Length == 0)
            {
                sqlKeys = $"[{key}]";
                sqlValues = $"{ GetDbPropertyValue(jToken[key]) }";
            }
            else
            {
                sqlKeys = $"{sqlKeys}, [{key}]";
                sqlValues = $"{sqlValues}, { GetDbPropertyValue(jToken[key]) }";
            }
        });

        return $"INSERT INTO [{tableName}] ({sqlKeys}) VALUES ({sqlValues});";
    }

    public static List<string> GetPropertyKeysForDynamic(JToken json)
    {
        Dictionary<string, object> values = json.ToObject<Dictionary<string, object>>();
        List<string> toReturn = new List<string>();
        foreach (string key in values.Keys)
        {
            toReturn.Add(key);                
        }
        return toReturn;
    }
}