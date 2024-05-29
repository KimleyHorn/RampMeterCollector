namespace RampMeterCollector
{
    class Program
    {
        static async Task Main()
        {
            var ramp = new RampCollector();
            await ramp.Connect();
        }
    }
}

