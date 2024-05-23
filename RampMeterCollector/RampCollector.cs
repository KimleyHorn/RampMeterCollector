using System;
using System.Data;
using System.Xml.Linq;
using Parquet;

namespace RampMeterCollector
{

    class RampCollector
    {
        private string url = "http://10.252.204.6/v1/asclog/xml/full?since=";

        public async Task Connect()
        {
            try
            {
                string xmlData = await CollectEvents();
                // Parse the XML data
                XDocument xmlDoc = XDocument.Parse(xmlData);
                Console.WriteLine("Root Element:");
                Console.WriteLine(xmlDoc.Root);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        public async Task<string> CollectEvents()
        {
            string formattedTime = GetFormattedTimeOneMinuteAgo();
            string requestUrl = url + formattedTime;

            Console.WriteLine($"Request URL: {requestUrl}");

            using (HttpClient client = new HttpClient())
            {
                HttpResponseMessage response = await client.GetAsync(requestUrl);

                if (response.IsSuccessStatusCode)
                {
                    string xmlData = await response.Content.ReadAsStringAsync();
                    return xmlData;
                }
                else
                {
                    string errorMessage = $"Failed to fetch XML data. Status code: {response.StatusCode}";
                    if (response.Content != null)
                    {
                        string responseBody = await response.Content.ReadAsStringAsync();
                        errorMessage += $"\nResponse body: {responseBody}";
                    }
                    throw new HttpRequestException(errorMessage);
                }
            }
        }

        #region Helper Methods

        private static string GetFormattedTimeOneMinuteAgo()
        {

        // Original DateTime
        DateTime originalDateTime = DateTime.Now;
        Console.WriteLine("Original DateTime (Local): " + originalDateTime);

        // Convert to DateTimeOffset
        DateTimeOffset localDateTimeOffset = new DateTimeOffset(originalDateTime);
        Console.WriteLine("Local DateTimeOffset: " + localDateTimeOffset);

        // Define the target time zone (e.g., Eastern Standard Time)
        TimeZoneInfo targetTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

        // Convert to the target time zone
        DateTimeOffset targetDateTimeOffset = TimeZoneInfo.ConvertTime(localDateTimeOffset, targetTimeZone);
        Console.WriteLine("Target DateTimeOffset (Eastern Standard Time): " + targetDateTimeOffset);
            // Get the current time and subtract one minute
            DateTime minusOne  = originalDateTime.AddMinutes(-1);

            // Format the datetime to the specified format
            string formattedTime = minusOne.ToString("MM-dd-yyyy H:mm:ss.f");

            return formattedTime;
        }

        #endregion


        #region Convert To Parquet
        static DataTable ConvertXmlToDataTable(string xmlData)
    {
        XDocument doc = XDocument.Parse(xmlData);
        DataTable dataTable = new DataTable("Items");

        foreach (XElement element in doc.Root.Elements())
        {
            DataRow row = dataTable.NewRow();
            foreach (XElement child in element.Elements())
            {
                if (!dataTable.Columns.Contains(child.Name.LocalName))
                {
                    dataTable.Columns.Add(child.Name.LocalName);
                }
                row[child.Name.LocalName] = child.Value;
            }
            dataTable.Rows.Add(row);
        }

        return dataTable;
    }

    static void WriteDataTableToParquet(DataTable dataTable, string filePath)
    {
        using (Stream fileStream = File.Create(filePath))
        {
            using (var parquetWriter = new ParquetWriter(new Parquet.Schema(dataTable), fileStream))
            {
                using (Parquet.RowGroupWriter rowGroupWriter = parquetWriter.CreateRowGroup())
                {
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        var values = new DataColumn[column.DataType];
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            values[i] = (DataColumn)dataTable.Rows[i][column];
                        }
                        rowGroupWriter.WriteColumn(new Parquet.Data.Column(column.ColumnName, values));
                    }
                }
            }
        }
    }
}
        #endregion
    }
}
