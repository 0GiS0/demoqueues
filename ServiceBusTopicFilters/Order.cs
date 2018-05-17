namespace ServiceBusTopicFilters
{
    internal class Order
    {
        public Order()
        {
        }

        public string Color { get; internal set; }
        public int Quantity { get; internal set; }
        public string Priority { get; internal set; }
    }
}