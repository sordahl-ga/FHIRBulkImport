﻿using System.Text;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.Extensions.Logging;
using System;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace FHIRBulkImport
{
    public static class FHIRUtils
    {
        private static object lockobj = new object();
        private static string _bearerToken = null;

        public static async System.Threading.Tasks.Task<FHIRResponse> CallFHIRServer(string path, string body, HttpMethod method, ILogger log)
        {
            if (!string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("FS-RESOURCE")) && ADUtils.isTokenExpired(_bearerToken))
            {
                lock (lockobj)
                {
                    if (ADUtils.isTokenExpired(_bearerToken))
                    {
                        log.LogInformation("Token is expired...Obtaining new bearer token...");
                        _bearerToken = ADUtils.GetOAUTH2BearerToken(System.Environment.GetEnvironmentVariable("FS-RESOURCE"), System.Environment.GetEnvironmentVariable("FS-TENANT-NAME"),
                                                                 System.Environment.GetEnvironmentVariable("FS-CLIENT-ID"), System.Environment.GetEnvironmentVariable("FS-SECRET")).GetAwaiter().GetResult();
                    }
                }
            }
            using (HttpClient _fhirClient = new HttpClient())
            {
                HttpRequestMessage _fhirRequest;
                HttpResponseMessage _fhirResponse;
                var fhirurl = $"{Environment.GetEnvironmentVariable("FS-URL")}/{path}";
                _fhirRequest = new HttpRequestMessage(method, fhirurl);
                _fhirClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _bearerToken);
                _fhirClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                _fhirRequest.Content = new StringContent(body, Encoding.UTF8, "application/json");
                _fhirResponse = await _fhirClient.SendAsync(_fhirRequest);
                return await FHIRResponse.FromHttpResponseMessage(_fhirResponse,log);
            }

        }
        public static string TransformBundle(string requestBody, ILogger log)
        {
            JObject result = JObject.Parse(requestBody);
            if (result == null || result["resourceType"] == null || result["type"] == null) return requestBody;
            string rtt = result.FHIRResourceType();
            string bt = (string)result["type"];
            if (rtt.Equals("Bundle") && bt.Equals("transaction"))
            {
                log.LogInformation($"TransformBundleProcess: looks like a valid transaction bundle");
                JArray entries = (JArray)result["entry"];
                if (entries.IsNullOrEmpty()) return result.ToString();
                log.LogInformation($"TransformBundleProcess: Phase 1 searching for existing entries on FHIR Server...");
                foreach (JToken tok in entries)
                {
                    if (!tok.IsNullOrEmpty() && tok["request"]["ifNoneExist"] != null)
                    {
                        string resource = (string)tok["request"]["url"];
                        string query = (string)tok["request"]["ifNoneExist"];
                        log.LogInformation($"TransformBundleProcess:Loading Resource {resource} with query {query}");
                        var r = FHIRUtils.CallFHIRServer($"{resource}?{query}", "", HttpMethod.Get, log).Result;
                        if (r.Success && r.Content !=null)
                        {
                            var rs = JObject.Parse(r.Content);
                            if (!rs.IsNullOrEmpty() && ((string)rs["resourceType"]).Equals("Bundle") && !rs["entry"].IsNullOrEmpty())
                            {
                                JArray respentries = (JArray)rs["entry"];
                                string existingid = "urn:uuid:" + (string)respentries[0]["resource"]["id"];
                                tok["fullUrl"] = existingid;
                            }
                        }
                    }
                }
                //reparse JSON with replacement of existing ids prepare to convert to Batch bundle with PUT to maintain relationships
                Dictionary<string, string> convert = new Dictionary<string, string>();
                result["type"] = "batch";
                entries = (JArray)result["entry"];
                foreach (JToken tok in entries)
                {
                    string urn = (string)tok["fullUrl"];
                    if (!string.IsNullOrEmpty(urn) && !tok["resource"].IsNullOrEmpty())
                    {
                        string rt = (string)tok["resource"]["resourceType"];
                        string rid = urn.Replace("urn:uuid:", "");
                        tok["resource"]["id"] = rid;
                        if (!convert.TryAdd(rid, rt))
                        {
                            //Duplicate catch
                            Guid g = Guid.NewGuid();
                            rid = g.ToString();
                            tok["resource"]["id"] = rid;

                        }
                        tok["request"]["method"] = "PUT";
                        tok["request"]["url"] = $"{rt}?_id={rid}";
                    }

                }
                log.LogInformation($"TransformBundleProcess: Phase 2 Localizing {convert.Count} resource entries...");
                string str = result.ToString();
                foreach (string id1 in convert.Keys)
                {
                    string r1 = convert[id1] + "/" + id1;
                    string f = "urn:uuid:" + id1;
                    str = str.Replace(f, r1);
                }
                return str;
            }
            return requestBody;
        }
    }
    public class FHIRResponse {
        public static async Task<FHIRResponse> FromHttpResponseMessage(HttpResponseMessage resp,ILogger log)
        {
            var retVal = new FHIRResponse();
            
            if (resp != null)
            {
                retVal.Content = await resp.Content.ReadAsStringAsync();
                retVal.Status = resp.StatusCode;
                retVal.Success = resp.IsSuccessStatusCode;
                if (!retVal.Success && string.IsNullOrEmpty(retVal.Content))
                {
                    retVal.Content = resp.ReasonPhrase;
                }
                IEnumerable<string> values;
                if (resp.Headers.TryGetValues("x-ms-retry-after-ms", out values))
                {
                    var s = values.First();
                    log.LogInformation($"x-ms Header:{s}");
                }
            }
            return retVal;
        }
        public FHIRResponse()
        {
            Status = System.Net.HttpStatusCode.InternalServerError;
            Success = false;
            Content = null;
        }
        public string Content { get; set; }
        public System.Net.HttpStatusCode Status {get;set;}
        public bool Success { get; set; }
    }
}
