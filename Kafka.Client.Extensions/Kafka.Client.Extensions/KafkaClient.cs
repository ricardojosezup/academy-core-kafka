using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Client.Extensions
{
    internal class KafkaClient<TKey, TValue> : IKafkaClient<TKey, TValue>
    {
        private ConsumerConfig _consumerConfig { get; set; }

        public Handle Handle => throw new NotImplementedException();

        public string Name => throw new NotImplementedException();

        private readonly IOptions<Config> _config;
        private readonly ILogger<KafkaClient<TKey, TValue>> _logger;
        //private readonly CustomCancellationTokenSource _cancellationTokenSource;

        public KafkaClient(IOptions<Config> config, ILogger<KafkaClient<TKey, TValue>> logger = null)
        {
            _config = config;
            _logger = logger;
            //_cancellationTokenSource = new CustomCancellationTokenSource();
            _consumerConfig = _config.Value.ConsumerConfig;
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(string Topic, Message<TKey, TValue> Message)
        {
            DeliveryResult<TKey, TValue> deliveryResult = null;
            using (var producerBuilder = new ProducerBuilder<TKey, TValue>(_config.Value.ProducerConfig)
                .SetErrorHandler((_, e) => _logger.LogError($"Error: {e.Reason}"))
                //.SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .Build())
            {
                try
                {
                    deliveryResult = await producerBuilder.ProduceAsync(Topic, Message);
                }
                catch (ProduceException<string, string> e)
                {
                    _logger.LogError($"failed to deliver message: {e.Message} [{e.Error.Code}] - {e.Error.Reason}");
                }
            }
            return deliveryResult;
        }

        public ConsumeResult<TKey, TValue> Consume(string Topic)
        {
            ConsumeResult<TKey, TValue> consumeResult = null;
            using (var consumerBuilder = new ConsumerBuilder<TKey, TValue>(_config.Value.ConsumerConfig)
                .SetErrorHandler((_, e) => _logger.LogError($"Error: {e.Reason}"))
                //.SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .Build())
            {
                consumerBuilder.Subscribe(Topic);
                try
                {
                    try
                    {
                        consumeResult = consumerBuilder.Consume();

                        consumerBuilder.Commit(consumeResult);
                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError($"Error occured: {e.Error.Reason}");
                    }
                }
                catch (OperationCanceledException)
                {
                    consumerBuilder.Close();
                }
            }
            return consumeResult;
        }



        public ConsumeResult<TKey, TValue> Consume(string Topic, int CommitPeriod)
        {
            //_consumerConfig.EnableAutoCommit = false;
            _consumerConfig.EnablePartitionEof = true;

            ConsumeResult<TKey, TValue> consumeResult = null;

            using (var consumer = new ConsumerBuilder<TKey, TValue>(_consumerConfig)
                .SetErrorHandler((_, e) => _logger.LogError($"Error: {e.Reason}"))
                //.SetStatisticsHandler((_, json) => _logger.LogInformation($"Statistics: {json}"))
                .Build())
            {
                consumer.Subscribe(Topic);

                try
                {
                    try
                    {
                        consumeResult = consumer.Consume();

                        if (consumeResult.IsPartitionEOF)
                        {
                            _logger.LogInformation(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                        }

                        _logger.LogInformation($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

                        if (consumeResult.Offset % CommitPeriod == 0)
                        {
                            try
                            {
                                if(_consumerConfig.EnableAutoCommit.HasValue && !_consumerConfig.EnableAutoCommit.Value)
                                    consumer.Commit(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                _logger.LogError($"Commit error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        _logger.LogError($"Consume error: {e.Error.Reason}");
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogError("Closing consumer.");
                    consumer.Close();
                }
            }
            return consumeResult;
        }

        //public void Dispose()
        //{
        //    _cancellationTokenSource.Cancel();
        //    if (!_cancellationTokenSource.IsDisposed)
        //        _cancellationTokenSource.Dispose();
        //}
    }
}