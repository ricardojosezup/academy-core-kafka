using Confluent.Kafka;
using System.Threading.Tasks;

namespace Kafka.Client.Extensions
{
    public interface IKafkaClient<TKey, TValue>// : IProducer<TKey, TValue>
    {
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(string Topic, Message<TKey, TValue> Message);
        ConsumeResult<TKey, TValue> Consume(string Topic);
        ConsumeResult<TKey, TValue> Consume(string Topic, int CommitPeriod);
    }
}