using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Temperature_Consumer
{
    internal class TemperatureModel
    {
        public int Temperature { get; set; }
        public long PollNumber { get; set; }
        public string Area { get; set; }
        public DateTime TimeStamp { get; set; }
    }
}
