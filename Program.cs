using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Temperature_Consumer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var configuration = new ConfigurationBuilder()
                    .SetBasePath(AppContext.BaseDirectory)
                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                    .Build();

                var config = new ConsumerConfig
                {
                    GroupId = configuration["BootstrapService:GroupId"],
                    BootstrapServers = $"{configuration["BootstrapService:Server"]}:{configuration["BootstrapService:Port"]}",
                    //AutoOffsetReset = AutoOffsetReset.Earliest
                };

                var connectionString = $"Host={configuration["DBConfiguration:Host"]};Port={configuration["DBConfiguration:Port"]};Username={configuration["DBConfiguration:Username"]};Password={configuration["DBConfiguration:Password"]};Database={configuration["DBConfiguration:Database"]}";
                using (var connection = new NpgsqlConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("Database Connection Successfull...");
                    Temperature temperature = new Temperature();
                    temperature.start(config, configuration, connection);
                }

                Console.ReadKey();
            }
            catch (NpgsqlException e)
            {
                Console.WriteLine("Database Error\n", -20);
                Console.WriteLine(e.Message.ToString());
                Console.WriteLine("", -20);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error\n", -20);
                Console.WriteLine(ex.Message.ToString());
                Console.WriteLine("", -20);
            }
        }
    }
}
