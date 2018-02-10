using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp_MongoDB
{
    public class SimpleOperation
    {
        private MongoDBService mongoDBService;
        private string collectionName;
        public SimpleOperation(MongoDBService mongoDBService, string collectionName)
        {
            this.mongoDBService = mongoDBService;
            this.collectionName = collectionName;
        }
        /// <summary>
        /// 创建BsonDocument
        /// 注意创建BsonDocument对象时，数字类型是不能用引号括起来的，应明确字段的类型，然后根据类型严格使用引号
        /// </summary>
        public BsonDocument CreateBsonDocument()
        {
            #region 错误写法

            //错误写法1.字符串未加引号
            /* 
             *“System.FormatException”类型的未经处理的异常在 MongoDB.Bson.dll 中发生 
             * 其他信息: Invalid JSON number '1A'.
            */
            string json1Error = @"{'Id': 1AAAA1}";
            //改正
            string json1Right = @"{'Id':'1AAAA1'}";


            //错误写法2.集合中的引号嵌套
            /*
             * System.FormatException”类型的未经处理的异常在 MongoDB.Bson.dll 中发生 
             * 其他信息: JSON reader was expecting ':' but found '':''.
             */
            string json2Error = @"{'Field2B': '[
                            {
                                'Field1E':'[]'
                            }
                        ]'}";
            //改正
            string json2Right = @"{'Field2B': [
                            {
                                'Field1E':[]
                            }
                        ]}";


            //错误写法3 构造键值对时，“:”使用中文输入法
            /*
             * System.FormatException”类型的未经处理的异常在 MongoDB.Bson.dll 中发生 
             * 其他信息: Invalid JSON input ''.
             */
            string json3Error = @"{'Id'：'1AAAA1'}";
            //改正
            string json3Right = @"{'Id':'1AAAA1'}";
            #endregion

            //将json转换为BsonDocument
            string json = @"{ 'Id':'100000000001',
                  'Field2A':'100',
                  'Field2B':'20160913', 
                  'Field2C':[
                    { 
                         'Field1A':'在MongoDB中一条记录是一个文档',
                         'Field1B':1,
                         'FieldC':13.14, 
                         'Field1D':'13.14F',
                         'Field1E':[]
                    }
                  ]
                }";

            BsonDocument doc1 = BsonDocument.Parse(json3Right);

            
            //也可以这样创建
            BsonDocument doc2 = new BsonDocument
            {
                 { "Id", "100000000000" },
                 { "Field2A","100"},
                 { "Field2B",DateTime.Now},
                 { "Field2C", new BsonArray
                    {
                         new BsonDocument("Field1A","MongoDB具有高性能，高可用性，高扩展性"),
                         new BsonDocument("Field1B",1),
                         new BsonDocument("Field1C",0.618),
                         new BsonDocument("Field1D",0.618F),
                         new BsonDocument("Field1E",new BsonArray()),
                    }
                 }
            };

            return doc2;
        }

        /// <summary>
        /// 插入文档
        /// </summary>
        public void Insert()
        {
            //Model2 model = new Model2 
            //{
            //    Id = Guid.NewGuid().ToString("N"),
            //    Field2A = "2",
            //    Field2B = DateTime.Now.Date,
            //    Field2C = new List<Model1>()
            //};

            //for (int i = 0; i < 3; i++)
            //{
            //    Model1 model1 = new Model1
            //    {
            //        Field1A = "Welcome to the MongoDB",
            //        Field1B = i,
            //        Field1C = 3.1415926,
            //        Field1D = 3.1415926F,
            //        Field1E = new List<string> { "asd","dsa","sad"}
            //    };
            //    model.Field2C.Add(model1);
            //}
            Model1 model1 = new Model1
            {
                Field1A = "Welcome to the MongoDB",
                Field1B = 0,
                Field1C = 3.1415926,
                Field1D = 3.1415926F,
                Field1E = new List<string> { "asd", "dsa", "sad" }
            };
            //插入一个collection
            bool t = mongoDBService.Insert(collectionName, model1);
        
        }

        /// <summary>
        /// 删除文档
        /// </summary>
        public void Delete()
        {
            mongoDBService.Delete<Model2>(collectionName, m => m.Field2A.Equals(DateTime.Parse("2016-09-08")));
        }
    }
}
