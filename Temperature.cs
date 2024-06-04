using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;

namespace Temperature_Consumer
{
    internal class Temperature
    {
        private List<TemperatureModel> dataList = new List<TemperatureModel>();
        private List<List<IncidentModel>> allIncidents = new List<List<IncidentModel>>();
        private List<IncidentModel> temperatureIncidents = new List<IncidentModel>();
        internal DateTime lastExecutionTime = DateTime.MinValue;
        int thresholdTime;
        int thresholdTemperature;
        private NpgsqlConnection connection;

        internal async Task start(ConsumerConfig _config, IConfiguration _configuration, NpgsqlConnection _connection)
        {
            Console.WriteLine("Temperature Consumer Started...");
            connection = _connection;
            Console.WriteLine("Configuration Checking...");
            checkConfiguration();
            Console.WriteLine("Configuration Fetched Successfully...");
            Console.WriteLine("Data Consuming Started...");
            await dataConsumer(_config, _configuration);
        }
        private async Task dataConsumer(ConsumerConfig _config, IConfiguration _configuration)
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe(_configuration["BootstrapService:Topic"]);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };
                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            var msg = JsonConvert.DeserializeObject<List<TemperatureModel>>(cr.Value.ToString());
                            dataList.AddRange(msg!);
                            await incidentChecker();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }

        private async Task incidentChecker()
        {
            if ((DateTime.Now - lastExecutionTime).TotalSeconds >= thresholdTime)
            {
                var groupedData = dataList.GroupBy(d => d.PollNumber);

                List<IncidentModel> incidents = new List<IncidentModel>();
                foreach (var group in groupedData)
                {
                    IncidentModel incident = new IncidentModel
                    {
                        PollNumber = group.Key,
                        AverageTemperature = group.Average(d => d.Temperature),
                        Area="",
                        Description="",
                        Threshold = thresholdTemperature,
                        StartTime = dataList.Min(d => DateTime.Parse(d.TimeStamp.ToString())),
                        EndTime = dataList.Max(d => DateTime.Parse(d.TimeStamp.ToString()))
                };
                    incidents.Add(incident);
                }
                dataList.Clear();
                allIncidents.Add(incidents);
                displayData(groupedData.Count());
                allIncidents.Clear();
                await checkConfiguration();
                lastExecutionTime = DateTime.Now;
            }
        }

        private async Task saveIncidents()
        {
            if (temperatureIncidents.Count > 0 && connection != null)
            {
                long[] pollNumber = temperatureIncidents.Select(model => model.PollNumber).ToArray();
                double[] averageTemperature = temperatureIncidents.Select(model => model.AverageTemperature).ToArray();
                string[] description = temperatureIncidents.Select(model => model.Description).ToArray();
                string[] area = temperatureIncidents.Select(model => model.Area).ToArray();
                DateTime[] starttime = temperatureIncidents.Select(model => model.StartTime).ToArray();
                DateTime[] endtime = temperatureIncidents.Select(model => model.EndTime).ToArray();
                temperatureIncidents.Clear();

                using (NpgsqlCommand cmd = new NpgsqlCommand($"select addtemperatureincidents(@in_pollnumber,@in_area,@in_description,@in_threshold,@in_interval,@in_starttime,@in_endtime);", connection))
                {
                    cmd.Parameters.Add(new NpgsqlParameter("in_pollnumber", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { Value = pollNumber.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_area", NpgsqlDbType.Array | NpgsqlDbType.Varchar) { Value = area.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_threshold", thresholdTemperature));
                    cmd.Parameters.Add(new NpgsqlParameter("in_interval", thresholdTemperature));
                    cmd.Parameters.Add(new NpgsqlParameter("in_description", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = description.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_starttime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = starttime.ToArray() });
                    cmd.Parameters.Add(new NpgsqlParameter("in_endtime", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = endtime.ToArray() });
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private async Task checkConfiguration()
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand($"select * from configurations where configurationid=2 and isdeleted=false;", connection))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        thresholdTemperature = reader.GetInt32(2);
                        thresholdTime = reader.GetInt32(3);
                    }
                }
            }
        }

        private async Task displayData(int count) {
            Console.Clear();
            Console.WriteLine($"\nThreshold Time = {thresholdTime} seconds");
            Console.WriteLine($"Threshold Speed = {thresholdTemperature} °C");
            double averageSpeed = dataList.Average(d => d.Temperature);
            Console.WriteLine($"Total Temperature Pools = {count}\n");

            foreach (var batchIncidents in allIncidents)
            {
                Console.ForegroundColor = ConsoleColor.Blue;
                Console.WriteLine("---------------------------------------------------------------------------------------------------------");
                Console.WriteLine($"| {"Poll Number",-15} | {"Area",-15} | {"Average Temperature",-12} | {"Start Time",-20} | {"End Time",-20} |");
                Console.WriteLine("---------------------------------------------------------------------------------------------------------");
                Console.ResetColor();

                foreach (var incident in batchIncidents)
                {
                    if (incident.AverageTemperature > thresholdTemperature)
                    {
                        Console.ForegroundColor = ConsoleColor.Black;
                        Console.BackgroundColor = ConsoleColor.Red;
                        temperatureIncidents.Add(incident);
                    }
                    else if (thresholdTemperature - incident.AverageTemperature <= 5)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Green;
                    }

                    Console.WriteLine($"| {incident.PollNumber,-15} | {incident.Area,-15} | {incident.AverageTemperature.ToString("F2")} {" °C",-14}| {incident.StartTime,-20} | {incident.EndTime,-20} |");
                    Console.ResetColor();
                }

                await saveIncidents();
                Console.ForegroundColor = ConsoleColor.Blue;
                Console.WriteLine("---------------------------------------------------------------------------------------------------------");
                Console.ResetColor();
                Console.Write("Loading...");
            }
        }
    }
}
