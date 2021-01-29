using System;
using System.IO;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.CompilerServices;
using Newtonsoft.Json.Linq;

namespace FHIRBulkImport
{
    public static class ImportFHIRBundles
    {
        [FunctionName("ImportFHIRBundles")]
        public static async Task Run([EventGridTrigger]EventGridEvent blobCreatedEvent,
                                     [Blob("{data.url}", FileAccess.Read, Connection = "FBI-STORAGEACCT")] Stream myBlob,
                                     ILogger log)
        {
            StorageBlobCreatedEventData createdEvent = ((JObject)blobCreatedEvent.Data).ToObject<StorageBlobCreatedEventData>();
            string name = createdEvent.Url.Substring(createdEvent.Url.LastIndexOf('/') + 1);
            if (myBlob == null) return;
            log.LogInformation($"ImportFHIRBUndles Processing file Name:{name} \n Size: {myBlob.Length}");
            var cbclient = StorageUtils.GetCloudBlobClient(System.Environment.GetEnvironmentVariable("FBI-STORAGEACCT"));
            StreamReader reader = new StreamReader(myBlob);
            var text = await reader.ReadToEndAsync();
            var fhirbundle = await FHIRUtils.CallFHIRServer("", FHIRUtils.TransformBundle(text,log), HttpMethod.Post, log);
            if (fhirbundle.Success && !LoadErrorsDetected(fhirbundle.Content,name,log))
            {
                await StorageUtils.MoveTo(cbclient, "bundles", "bundlesprocessed", name, $"{name}.processed",log);
                await StorageUtils.WriteStringToBlob(cbclient, "bundlesprocessed", $"{name}.result", fhirbundle.Content, log);
            } else
            {
                await StorageUtils.MoveTo(cbclient, "bundles", "bundleserr", name,$"{name}.err", log);
                await StorageUtils.WriteStringToBlob(cbclient, "bundleserr", $"{name}.err.result", fhirbundle.Content, log);

            }
            log.LogInformation($"ImportFHIRBUndles Processed file Name:{name}");
        }
        public static bool LoadErrorsDetected(string response, string name, ILogger log)
        {
            try
            {
                log.LogInformation($"ImportFHIRBundles:Checking for load errors file {name}");
                JObject o = JObject.Parse(response);
                if (o["entry"] != null)
                {
                    JArray entries = (JArray)o["entry"];
                    foreach (JToken tok in entries)
                    {
                        if (tok["response"] != null && tok["response"]["status"] != null)
                        {
                            string s = (string)tok["response"]["status"];
                            int rc = 200;
                            if (int.TryParse(s, out rc))
                            {
                                if (rc < 200 || rc > 299)
                                {
                                    log.LogError($"ImportFHIRBundles: Load error(s) detected for resource(s) in bundle response for {name}");
                                    return true;
                                }
                            }
                        }
                    }
                    return false;

                }
                log.LogError($"ImportFHIRBundles: Cannot detect resource entries in response for {name}");
                return true;
            }
            catch (Exception e)
            {
                log.LogError($"ImportFHIRBundles: Unable to parse server response to check for errors file {name}:{e.Message}");
                return true;
            }
        }
    }
}
