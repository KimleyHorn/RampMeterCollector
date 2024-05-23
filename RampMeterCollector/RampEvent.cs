namespace RampMeterCollector
{
    public class RampEvent
    {
        private int _id;
        private DateTime _timeStamp;
        private int _eventTypeId;
        private int _parameter;

        public int Id
        {
            get => _id;
            set => _id = value;
        }

        public DateTime TimeStamp
        {
            get => _timeStamp;
            set => _timeStamp = value;
        }

        public int Parameter
        {
            get => _parameter;
            set => _parameter = value;
        }
        public int EventTypeId
        {
            get => _eventTypeId;
            set => _eventTypeId = value;
        }
    }
}
