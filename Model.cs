using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp_MongoDB
{
    /// <summary>
    /// 没有子文档的模型
    /// </summary>
    public class Model1
    {
        public string Field1A { set; get; }
        public int Field1B { set; get; }
        public double Field1C { set; get; }
        public float Field1D { set; get; }
        public List<string> Field1E { set; get; }
    }

    /// <summary>
    /// 含有子文档和_id字段
    /// </summary>
    public class Model2
    {
        public string Id { set; get; }
        public string Field2A { set; get; }
        public DateTime Field2B { set; get; }

        public List<Model1> Field2C { set; get; }
    }
}
