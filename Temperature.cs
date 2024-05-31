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
        int thresholdTime = 10;
        int thresholdTemperature = 45;
        private NpgsqlConnection connection;

        internal async Task dataConsumer(ConsumerConfig _config, IConfiguration _configuration, NpgsqlConnection _connection)
        {
            connection = _connection;
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

                DateTime startTime = dataList.Min(d => DateTime.Parse(d.TimeStamp.ToString()));
                DateTime endTime = dataList.Max(d => DateTime.Parse(d.TimeStamp.ToString()));

                var groupedData = dataList.GroupBy(d => d.PollNumber);

                Console.Clear();
                Console.WriteLine($"\nThreshold Time = {thresholdTime} seconds");
                Console.WriteLine($"Threshold Speed = {thresholdTemperature} °C");
                double averageSpeed = dataList.Average(d => d.Temperature);
                Console.WriteLine($"Total Temperature Pools = {groupedData.Count()}\n");

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
                        StartTime = startTime,
                        EndTime = endTime
                    };
                    incidents.Add(incident);
                }

                allIncidents.Add(incidents);

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

                dataList.Clear();
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

        internal async Task checkConfiguration()
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
    }
}
