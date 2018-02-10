using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Linq;

namespace ConsoleApp_MongoDB
{
    public class ComplexOperation
    {
        private MongoDBService mongoDBService;
        private string collectionName;
        public ComplexOperation(MongoDBService mongoDBService, string collectionName)
        {
            this.mongoDBService = mongoDBService;
            this.collectionName = collectionName;
        }
        /// <summary>
        /// 插入子文档
        /// </summary>
        public void InsertChild()
        {
            Model1 model1 = new Model1
            {
                Field1A = "MongoDB是一种开源文档型数据库",
                Field1B = 100,
                Field1C = 3.1415926,
                Field1D = 3.1415926F,
                Field1E = new List<string>()
            };

            Model2 model2 = new Model2
            {
                Id = new Guid().ToString("N"),
                Field2A = "1",
                Field2B = DateTime.Now.Date,
                Field2C = new List<Model1>()
            };
            

            FilterDefinitionBuilder<Model2> filterBuilder = Builders<Model2>.Filter;
            //过滤条件字段Field2A==2
            FilterDefinition<Model2> filter = filterBuilder.Eq(doc => doc.Field2A, "2");
            SortDefinitionBuilder<Model1> sortBuilder = Builders<Model1>.Sort;
            //按字段Field1A升序排列
            SortDefinition<Model1> sort = sortBuilder.Ascending(pu => pu.Field1A);
            //最新插入的在最前面
            UpdateDefinitionBuilder<Model2> updateBuilder = Builders<Model2>.Update;
            //PushEach 参数含义：
            //field:lambda表达式，要更新的字段，这里其实是个集合，集合里的每一个元素都是一个子文档。
            //values:待放入field的值，这里是一个集合，所以一次可以放入多个
            //slice:约束子文档集合的大小，如果设置为null,则大小不限，如果设置为0，那么没有任何子文档可被装入其中。
            //position:指定插入子文档在集合中的位置
            //sort:对子文档进行排序，可以指定任意字段排序
            UpdateDefinition<Model2> update = updateBuilder.PushEach(doc => doc.Field2C, new List<Model1> { model1 }, null, 0, sort);
            UpdateResult updateResult = mongoDBService.DocumentUpdate(collectionName, filter, update);
        }

        /// <summary>
        /// 更改某一子文档
        /// </summary>
        public void Update()
        {
            //更新子文档的字段
            string commentContent = "通过Update修改了";
            FilterDefinitionBuilder<Model2> filterBuilder = Builders<Model2>.Filter;
            //找到父文档，过滤条件为Field2A=2并且Field2B=“2018-01-21T16:00:00.000Z”
            FilterDefinition<Model2> filterFather = filterBuilder.Eq(doc => doc.Field2A, "2")
                & filterBuilder.Eq(doc => doc.Field2B, DateTime.Parse("2018-01-21T16:00:00.000Z"));
            //找到子文档，过滤条件Field1B=1，条件作用与字段Field2C，他是个集合，用来存储子文档
            FilterDefinition<Model2> childFilter = filterBuilder.ElemMatch(
                listField => listField.Field2C, childfield => childfield.Field1B == 1);
            //上述条件的并
            FilterDefinition<Model2> filter = Builders<Model2>.Filter.And(new FilterDefinition<Model2>[] { filterFather, childFilter });

            //方法1：使用XXXX.$.XXXX定位字段
            var update = Builders<Model2>.Update.Set("Field2C.$.Field1A", commentContent);

            UpdateResult updateResult = mongoDBService.DocumentUpdate(collectionName, filter, update);
        }

        /// <summary>
        /// 查找某一个子文档
        ///</summary>
        public void QueryChild()
        {
            FilterDefinitionBuilder<Model2> filterBuilder = Builders<Model2>.Filter;
            //找到父文档，过滤条件为Field2A=2并且Field2B=“2018-01-21T16:00:00.000Z”
            FilterDefinition<Model2> filterFather = filterBuilder.Eq(doc => doc.Field2A, "2")
                & filterBuilder.Eq(doc => doc.Field2B, DateTime.Parse("2018-01-21T16:00:00.000Z"));
              

            //投影定义创建器：ProjectionDefinitionBuilder
            //用ProjectionDefinition过滤子文档,投影器创建器作用于Field2C，他是一个集合，用来保存多个子文档；过滤条件为Field1C = 3.1415926
            ProjectionDefinitionBuilder<Model2> projBuilder = Builders<Model2>.Projection;
            ProjectionDefinition<Model2> proj = projBuilder.ElemMatch(listField => listField.Field2C, childfield => childfield.Field1C == 3.1415926);

            FindOptions options = new FindOptions() { AllowPartialResults = true };
            Model2 info = mongoDBService.Single<Model2>(collectionName, filterFather, options, proj);
        }
        /// <summary>
        /// 获得所有子文档
        /// 这种方法不能够获得指定条件的所有子文档，要想选出想要的子文档要用聚集操作
        /// </summary>
        public void QueryChildren()
        {
            //投影定义创建器：ProjectionDefinitionBuilder
            //用ProjectionDefinition过滤子文档
            ProjectionDefinitionBuilder<Model2> projBuilder = Builders<Model2>.Projection;
            ProjectionDefinition<Model2> proj = projBuilder.ElemMatch(listField => listField.Field2C, childfield => childfield.Field1B ==0);

            List<Model2> info = mongoDBService.List<Model2>(collectionName,
                m => m.Field2A == "2" && m.Field2B == DateTime.Parse("2018-01-21T16:00:00.000Z"), proj);
        }

        /// <summary>
        /// 文档排序
        /// </summary>
        public void Sort()
        {
            SortDefinitionBuilder<Model2> sortBuilder = Builders<Model2>.Sort;
            ////按字段Field2A降序排列
            SortDefinition<Model2> sort = sortBuilder.Descending(m => m.Field2A);

            List<Model2> info = mongoDBService.FindAllList(collectionName, sort);
        }

        /// <summary>
        /// 内嵌文档分页
        /// </summary>
        public void SunPageList()
        {
            //投影定义创建器：ProjectionDefinitionBuilder
            //用ProjectionDefinition过滤子文档
            ProjectionDefinitionBuilder<Model2> projBuilder = Builders<Model2>.Projection;
            //Include :确定要包含哪些字段值（即给哪些字段赋值）
            //Slice:获得子文档集合分片，第一个参数field指取出的子文档集合，第二各参数skip指跳过多少个子文档，第三个参数limit取出多少个
            ProjectionDefinition<Model2> proj = projBuilder.Include(m => m.Field2C).Slice(m => m.Field2C, 1, 3);
            //过滤条件是Field2A=2
            Model2 doc = mongoDBService.SubdocumentPageList<Model2>(collectionName, m => m.Field2A == "2", proj);
        }

        /// <summary>
        /// 删除一个子文档
        /// </summary>
        public void DeleteSubDoc()
        {
            //过滤器作用与Field2C字段，过滤条件是Field1B = 1
            var update = Builders<Model2>.Update.PullFilter(m => m.Field2C, (y => y.Field1B == 2));
            //父文档过滤条件为Field2A=2，如果匹配出多个父文档，只操作第一个文档
            mongoDBService.UpdateOne<Model2>(collectionName, m => m.Field2A == "2", update);
        }
        /// <summary>
        /// 删除集合元素
        /// </summary>
        public void DeleteArray()
        {
            FilterDefinitionBuilder<Model1> filterBuilder = Builders<Model1>.Filter;
            FilterDefinition<Model1> filter = filterBuilder.Eq(doc => doc.Field1B, 0);
            UpdateDefinition<Model1> update = Builders<Model1>.Update.Pull(m => m.Field1E, "asd");
            mongoDBService.UpdateOne<Model1>(collectionName, filter, update);
        }
        /// <summary>
        /// 聚集操作,筛选数据,第一种写法
        /// 注意： "SubscriberId",1 这里的1不能加引号，否则查不到数据；其他情况与此相似。
        /// </summary>
        public void GetSubDocument1()
        {
            //unwind阶段
            var unwind = new BsonDocument{
                     {
                       "$unwind","$Field2C"
                     }
                  };
            //match阶段。匹配条件Field2A=2，Field1B=1
            //注意Field2A为字符串类型，2用引号包起来；而Field1B为整形，所以1不能用引号包起来
            var match = new BsonDocument
                {
                    {
                        "$match",
                        new BsonDocument
                        {
                            {
                                "Field2A","2"                             
                            }
                            ,
                             {
                                "Field2C.Field1B",1                         
                            }
                        }
                    }
                };
            //group阶段
            var group = new BsonDocument
                {
                    {
                        "$group",
                        new BsonDocument
                        {
                            {
                                "_id","$Field1C"
                            },
                            {
                              "Field2C",
                               new BsonDocument
                              {
                                 {
                                   "$push","$Field2C"
                                 }
                             }
                        }
                        }                     
                        
                    }
                };
            var r = mongoDBService.Aggregate<Model2>(collectionName, new BsonDocument[] { unwind, match, group });
        }
        /// <summary>
        /// 聚集操作,筛选数据,第二种写法
        /// </summary>
        public void GetSubDocumentTwo()
        {
            var match = new BsonDocument
                {
                    {"SubscriberId",1},
                             {
                                "PublishInfoList.FollowType",3                             
                            }
                };
            var group = new BsonDocument
                {
                    { "_id","$SubscriberId"},
                    { "PublishInfoList",
                        new BsonDocument
                        {
                            {"$push","$PublishInfoList"}
                        }
                    }
                };
            var r = mongoDBService.Aggregate(collectionName, "PublishInfoList", match, group);
        }

        /// <summary>
        /// 操作多个集合，从每个集合中取出m条数据，按时间排序
        /// </summary>
        public void GetData0()
        {
            List<Model1> infoList = new List<Model1>();
            List<int> subId = new List<int> { 1, 2 };
            foreach (int id in subId)
            {
                var unwind = new BsonDocument{
                     {
                       "$unwind","$PublishInfoList"
                     }
                  };
                var sort = new BsonDocument {
                    {
                        "CreateTime",1
                    }
                };
                var match = new BsonDocument
                {
                    {
                        "$match",
                        new BsonDocument
                        {
                            {
                                "SubscriberId",id                            
                            }
                            ,
                             {
                                "PublishInfoList.FollowType",1                      
                            }
                        }
                    }
                };
                var group = new BsonDocument
                {
                    {
                        "$group",
                        new BsonDocument
                        {
                            {
                                "_id","$SubscriberId"
                            },
                            {
                              "PublishInfoList",
                               new BsonDocument
                              {
                                 {
                                   "$push","$PublishInfoList"
                                 }
                             }
                        }
                        }                     
                        
                    }
                };
                var project = new BsonDocument 
            {
                {"$project",new BsonDocument{{"PublishInfoList",1}}}
            };
                var limit = new BsonDocument
                    {
                        {
                            "$limit",9
                        }
                    };
                var r = mongoDBService.Aggregate<Model2>(collectionName, new BsonDocument[] { unwind, match, project, limit });
                //infoList.AddRange(r[0].PublishInfoList);
            }
        }

        /// <summary>
        /// 获得数组大小
        /// </summary>
        public void GetArrayCount()
        {
            var match = new BsonDocument
            {
                {
                    "$match",new BsonDocument{{"Field2A","2"}}
                }
            };

            var project = new BsonDocument
            {
                {
                    "$project",new BsonDocument
                    {
                        {
                            "NumofArray",
                            new BsonDocument
                            {
                                { "$size", "$Field2C" }
                            }
                        }
                    }
                }
                
            };
            BsonDocument bson = mongoDBService.Aggregate<BsonDocument>(collectionName, 
                new BsonDocument[] { match, project }).FirstOrDefault<BsonDocument>();
            int count = bson != null?bson.GetValue("NumofArray").AsInt32:0;//获得集合MessageIdList的大小
        }
    }
}