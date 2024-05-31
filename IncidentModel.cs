using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Temperature_Consumer
{
    internal class IncidentModel
    {
        public double AverageTemperature { get; set; }
        public long PollNumber { get; set; }
        public string Area { get; set; }
        public string Description { get; set; }
        public int Threshold { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }
}
