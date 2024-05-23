using System.Configuration;
using System.Collections.Generic;
using System.Globalization;
using System.Xml.Linq;


namespace RampMeterCollector
{

    public class RampCollector
    {
        private List<RampEvent> _rampEvents;
        private string day;
        private string url = "http://10.252.204.6/v1/asclog/xml/full?since=";


        public RampCollector()
        {
            day = "05-21-2024%209:00:00.0";
        }

        public async Task Connect(string connectionString)
        {
            try
            {
                string xmlData = await CollectEvents();
                Console.WriteLine("XML Data:");
                Console.WriteLine(xmlData);

                // Parse the XML data
                XDocument xmlDoc = XDocument.Parse(xmlData);
                // Do something with the parsed XML data
                // For example, print the root element
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
            using (HttpClient client = new HttpClient())
            {
                HttpResponseMessage response = await client.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    string xmlData = await response.Content.ReadAsStringAsync();
                    return xmlData;
                }
                else
                {
                    throw new HttpRequestException($"Failed to fetch XML data. Status code: {response.StatusCode}");
                }
            }
        }


        #region Helper Methods

        private DateTime GetTime(string input)
        {
            // Replace %20 with a space
            input = input.Replace("%20", " ");

            // Define the format of the input string
            string format = "MM-dd-yyyy HH:mm:ss.f";

            // Parse the input string to a DateTime object
            if (DateTime.TryParseExact(input, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
            {
                return result;
            }
            else
            {
                throw new FormatException("The input string is not in the correct format.");
            }
        }

        

        #endregion

    }
}
