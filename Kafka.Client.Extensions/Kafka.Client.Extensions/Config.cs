using Confluent.Kafka;

namespace Kafka.Client.Extensions
{
    public class Config
    {
        public ProducerConfig ProducerConfig { get; set; }
        public ConsumerConfig ConsumerConfig { get; set; }
    }
}
