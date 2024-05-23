using System.Xml.Linq;

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
    }
}
