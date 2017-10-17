#r "Newtonsoft.Json"

using System.Net;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Linq;

public static HttpResponseMessage Run(HttpRequestMessage req, string tableName, int? id, TraceWriter log)
{
    log.Info("C# HTTP trigger function processing a request...");

    var sqlCmd = "";
    var method = req.Method.ToString();

    switch (method) {
        case "GET":
            log.Info("GET");
            sqlCmd = Processor.Get(tableName, id);
            break;
        case "POST":
            log.Info("POST");
            sqlCmd = Processor.Add(tableName, req.Content);
            break;
        // case "PATCH":
        //     log.Info("POST");
        //     break;
        // case "DELETE":
        //     log.Info("PSOT");
        //     break;
        // case "PUT":
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

    public static JObject ParseJsonFromContent(HttpContent content){
        string jsonContent = content.ReadAsStringAsync().Result;
        if (jsonContent.Length > 0) {
            return JObject.Parse(jsonContent);
        }
        return null;
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
        var json = ParseJsonFromContent(content);
        var sql = "";

        if (json != null){
            List<string>keyList = GetPropertyKeysForDynamic(json);
            var sqlKeys = "";
            var sqlValues = "";
            keyList.ForEach(key => {
                if (sqlKeys.Length == 0){
                    sqlKeys = $"[{key}]";
                    sqlValues = $"{ GetDbPropertyValue(json[key]) }";
                } else {
                    sqlKeys = $"{sqlKeys}, [{key}]";
                    sqlValues = $"{sqlValues}, { GetDbPropertyValue(json[key]) }";
                }
            });

            sql = $"INSERT INTO [{tableName}] ({sqlKeys}) VALUES ({sqlValues})";
        }

        return sql;
    }

    public static List<string> GetPropertyKeysForDynamic(JObject json)
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