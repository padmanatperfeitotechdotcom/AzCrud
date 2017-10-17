#r "Newtonsoft.Json"

using System.Net;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

public static HttpResponseMessage Run(HttpRequestMessage req, string tableName, int? id, TraceWriter log)
{
    var sqlCmd = "";
    var method = req.Method.ToString();

    switch (method) {
        case "GET":
            log.Info("SELECT");
            sqlCmd = Processor.Get(tableName, id);
            break;
        case "POST":
            log.Info("INSERT");
            sqlCmd = Processor.Add(tableName, req.Content);
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


    log.Info(sqlCmd);
    
    return req.CreateResponse(HttpStatusCode.OK, sqlCmd);
}

public static class Processor {
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