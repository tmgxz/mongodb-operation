using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp_MongoDB
{
    class Program
    {
        static void Main(string[] args)
        {
            //MongoDBService mongoDBService = new MongoDBService("mongodb://localhost:27017/?maxPoolSize=100&minPoolSize=10", "TestDB");

            //SimpleOperation so = new SimpleOperation(mongoDBService, "commonColl");
            //so.Insert();
            //ComplexOperation co = new ComplexOperation(mongoDBService, "commonColl");
            //co.DeleteArray();
            
            //GridFSOperation gop = new GridFSOperation(mongoDBService);
            //gop.UpLoad();

            //Console.Read();

            //var t = DateTime.Parse("2018-01-21T16:00:00.000Z");
            Show.Exec();
        }
    }
}
