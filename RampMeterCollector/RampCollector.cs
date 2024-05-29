using System.Data;
using System.Xml.Linq;
using Parquet;
using Parquet.Schema;
using System.Configuration;
using MySqlConnector;

namespace RampMeterCollector
{

    public class RampCollector
    {
        private static readonly string Url = ConfigurationManager.AppSettings["URL"] ?? "";
        private static readonly int ThreadCount = int.Parse(ConfigurationManager.AppSettings["THREAD_COUNT"] ?? "1");
        internal static readonly string MySqlDbName = ConfigurationManager.AppSettings["DB_NAME"] ?? "mark1";
        internal static readonly string? MySqlConnString = ConfigurationManager.AppSettings["CONN_STRING"];
        internal static MySqlConnection MySqlConnection;
        internal string RampString = string.Empty;
        internal List<RampEvent> RampEvents = [];
        internal string HistoryDir = ConfigurationManager.AppSettings["HISTORY_DIR"] ?? "C:\\";

        #region Startup

        static RampCollector()
        {
            MySqlConnection = new MySqlConnection(MySqlConnString);
        }

        public async Task Connect()
        {
            try
            {
                var xmlData = await CollectEvents();
                var xmlDoc = XDocument.Parse(xmlData);
                ConvertXmlToEvent(xmlDoc);
                await WriteEventsToParquet(RampEvents, "Ramp Meter History");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        #endregion


        public async Task<string> CollectEvents()
        {
            var formattedTime = GetFormattedTimeOneMinuteAgo();
            var requestUrl = Url + formattedTime;
            using var client = new HttpClient();
            var response = await client.GetAsync(requestUrl);

            if (response.IsSuccessStatusCode)
            {
                var xmlData = await response.Content.ReadAsStringAsync();
                RampString += xmlData;
                return xmlData;
            }

            var errorMessage = $"Failed to fetch XML data. Status code: {response.StatusCode}";
            if (response.Content == null) throw new HttpRequestException(errorMessage);
            var responseBody = await response.Content.ReadAsStringAsync();
            errorMessage += $"\nResponse body: {responseBody}";
            throw new HttpRequestException(errorMessage);

        }

        #region Helper Methods

        private static string GetFormattedTimeOneMinuteAgo()
        {
            var originalDateTime = DateTime.Now;
            var localDateTimeOffset = new DateTimeOffset(originalDateTime);
            var targetTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            TimeZoneInfo.ConvertTime(localDateTimeOffset, targetTimeZone);
            var minusOne = originalDateTime.AddMinutes(-1);
            var formattedTime = minusOne.ToString("MM-dd-yyyy H:mm:ss.f");

            return formattedTime;
        }

        private Task ConvertParquetToCSV()
        {

            return null;
        }

        public void ConvertXmlToEvent(XDocument doc)
        {
            if (string.IsNullOrEmpty(RampString))
            {
                Console.WriteLine("No Ramp Events");
                return;
            }

            var events = doc.Descendants("EventResponses")
                            .Descendants("Event")
                            .Select(e => new RampEvent
                            {
                                Id = (int)e.Attribute("ID"),
                                TimeStamp = DateTime.ParseExact((string)e.Attribute("TimeStamp"), "MM-dd-yyyy HH:mm:ss.f", null),
                                EventTypeId = (int)e.Attribute("EventTypeID"),
                                Parameter = (int)e.Attribute("Parameter")
                            })
                            .ToList();

            RampEvents.AddRange(events);
#if DEBUG
            foreach (var rampEvent in events)
            {
                Console.WriteLine($"ID: {rampEvent.Id}, TimeStamp: {rampEvent.TimeStamp}, Event Type: {rampEvent.EventTypeId}, Parameter: {rampEvent.Parameter}");
            }
#endif
        }
        #endregion

        #region Convert To Parquet

        private async Task WriteEventsToParquet(List<RampEvent> events, string folderName)
        {
            var fullPath = Path.Combine(HistoryDir, folderName);

            if (!Directory.Exists(fullPath))
            {
                Directory.CreateDirectory(fullPath);
            }
            var filePath = Path.Combine(fullPath, $"ramp_meter_events_{DateTime.Now:yyyy-MM-dd_HHmmss}.parquet");

            var fields = new List<DataField>
                {
                    new DataField<int>("Id"),
                    new DateTimeDataField("TimeStamp", DateTimeFormat.DateAndTime),
                    new DataField<int>("EventTypeId"),
                    new DataField<int>("Parameter")
                };

            await using Stream fileStream = File.Create(filePath);
            using var parquetWriter = await ParquetWriter.CreateAsync(new ParquetSchema(fields.ToArray()), fileStream);
            using var rowGroupWriter = parquetWriter.CreateRowGroup();
            await WriteColumnAsync(rowGroupWriter, fields[0], events.Select(e => e.Id).ToArray());
            await WriteColumnAsync(rowGroupWriter, fields[1], events.Select(e => e.TimeStamp).ToArray());
            await WriteColumnAsync(rowGroupWriter, fields[2], events.Select(e => e.EventTypeId).ToArray());
            await WriteColumnAsync(rowGroupWriter, fields[3], events.Select(e => e.Parameter).ToArray());
        }

        private static async Task WriteColumnAsync<T>(ParquetRowGroupWriter rowGroupWriter, DataField field, T[] data)
        {
            var dataColumn = new Parquet.Data.DataColumn(field, data);
            await rowGroupWriter.WriteColumnAsync(dataColumn);
        }

        #endregion

        #region Error Logging

        /// <summary>
        /// The overloaded method that will write to the error log
        /// </summary>
        /// <param name="applicationName">The name of the file the error is coming from</param>
        /// <param name="functionName">The name of the function the error is coming from</param>
        /// <param name="ex">The exception being thrown</param>
        /// <returns>A task since the method is asynchronous</returns>
        public static async Task WriteToErrorLog(string applicationName,
         string functionName, Exception ex)
        {
            await WriteToErrorLog(applicationName, functionName, ex.Message,
                ex.InnerException?.ToString());
        }

        /// <summary>
        /// The private method that will write to the error log in the database
        /// </summary>
        /// <param name="applicationName">The name of the file the error is coming from</param>
        /// <param name="functionName">The name of the function the error is coming from</param>
        /// <param name="exception">The exception being thrown</param>
        /// <param name="innerException">The inner exception being thrown (if applicable)</param>
        /// <returns>A task since the method is asynchronous</returns>
        private static async Task WriteToErrorLog(string applicationName,
            string functionName, string exception, string? innerException)
        {
            try
            {
                if (MySqlConnection.State == ConnectionState.Closed)
                {
                    await MySqlConnection.OpenAsync();
                }
                await using var cmd = new MySqlCommand();

                cmd.Connection = MySqlConnection;
                cmd.CommandText =
                    $"insert into {MySqlDbName}.errorlog (applicationname, functionname, exception, innerexception) values ('{applicationName}', '{functionName}', " +
                    $"'{exception[..(exception.Length > 500 ? 500 : exception.Length)]}', " +
                    $"'{innerException}') ";
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally { await MySqlConnection.CloseAsync(); }
        }

        #endregion Error Logging
    }
}
