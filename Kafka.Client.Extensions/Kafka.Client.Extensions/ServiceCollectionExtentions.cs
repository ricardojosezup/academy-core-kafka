using Microsoft.Extensions.DependencyInjection;
using System;

namespace Kafka.Client.Extensions
{
    public static class ServiceCollectionExtentions
    {
        public static IServiceCollection AddKafka(this IServiceCollection services, Action<Config> options)
        {
            services.Configure<Config>(options);
            services.AddSingleton(typeof(IKafkaClient<,>), typeof(KafkaClient<,>));

            return services;
        }
    }
}