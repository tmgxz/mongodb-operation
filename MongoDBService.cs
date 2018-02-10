using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Linq.Expressions;

using MongoDB;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Threading.Tasks;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using System.Collections.Concurrent;
using System.Threading;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using System.IO;

namespace ConsoleApp_MongoDB
{
    /// <summary>
    /// MongoDB操作
    /// </summary>
    public class MongoDBService
    {
        #region 变量
        /// <summary>
        /// 缓存
        /// </summary>
        private static ConcurrentDictionary<string, Lazy<MongoClient>> m_mongoClientCache = 
            new ConcurrentDictionary<string, Lazy<MongoClient>>();
        /// <summary>
        /// 连接字符串
        /// </summary>
        private string m_connectionStr = string.Empty;
        /// <summary>
        /// 数据库名称
        /// 支持运行时更改
        /// </summary>
        public string DatabaseName { get; set; }
        /// <summary>
        /// 设置GridFS参数
        /// </summary>
        public GridFSBucketOptions BucksOptions { get; set; }
        #endregion

       
        /// <summary>
        /// 初始化操作
        /// </summary>
        public MongoDBService(string connStr, string database)
        {
            m_connectionStr = connStr;
            DatabaseName = database;
        }

        /// <summary>
        /// 获得Mongo客户端
        /// </summary>
        /// <param name="connStr">连接串</param>
        /// <returns></returns>
        private static MongoClient GetClient(string connStr)
        {
            if (string.IsNullOrWhiteSpace(connStr)) throw new ArgumentException("MongoDB Connection String is Empty");

            return m_mongoClientCache.GetOrAdd(connStr,
                new Lazy<MongoClient>(() =>
                {
                    return new MongoClient(connStr);
                })).Value;
        }

        #region 插入操作
        /// <summary>
        /// 插入操作
        /// </summary>
        /// <param name="collectionName">集合名</param>
        /// <param name="t">插入的对象</param>
        /// <returns>返回是否插入成功true or false</returns>
        public bool Insert<T>(string collectionName, T t)
        {
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("collectionName是null、空或由空白字符组成");
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            Task task = collection.InsertOneAsync(t);
            task.Wait();
            return !task.IsFaulted;
        }

        /// <summary>
        /// 插入多个实体
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="list"></param>
        /// <returns></returns>
        public bool InsertMany<T>(string collectionName, List<T> list)
        {
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("collectionName是null、空或由空白字符组成");
            if (list == null) throw new ArgumentException("list是null");
            if (list.Count == 0) throw new ArgumentException("list大小为0");

            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            Task task = collection.InsertManyAsync(list);
            task.Wait();
            return !task.IsFaulted;
        }
        /// <summary>
        /// 插入多个Json
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="jsonList">json链表</param>
        /// <returns></returns>
        public bool InsertMany(string collectionName, List<string> jsonList)
        {
            if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("collectionName是null、空或由空白字符组成");
            if (jsonList == null) throw new ArgumentException("list是null");
            if (jsonList.Count == 0) throw new ArgumentException("list大小为0");

            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<BsonDocument>(collectionName);
            List<BsonDocument> bsonList = new List<BsonDocument>();
            foreach (string jsonStr in jsonList)
            {
                using (var jsonReader = new JsonReader(jsonStr))
                {
                    var context = BsonDeserializationContext.CreateRoot(jsonReader);
                    var document = collection.DocumentSerializer.Deserialize(context);
                    bsonList.Add(document);
                }
            }
            Task task = collection.InsertManyAsync(bsonList);
            task.Wait();
            return !task.IsFaulted;
        }
        #endregion

        #region 更新操作
        /// <summary>
        /// 更新操作
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="t">更新对象</param>
        /// <param name="filter">条件</param>
        /// <returns>更新是否成功true or false</returns>
        public bool UpdateOne<T>(string collectionName, Expression<Func<T, bool>> filter, T t)
        {
            if (t == null) return false;

            var itemBson = t.ToBsonDocument();
            var updateDef = new BsonDocumentUpdateDefinition<T>(new BsonDocument("$set", itemBson));

            return UpdateOne<T>(collectionName, filter, updateDef);
        }
        /// <summary>
        /// 更新操作
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <param name="updateDef"></param>
        /// <returns></returns>
        public bool UpdateOne<T>(string collectionName, Expression<Func<T, bool>> filter, UpdateDefinition<T> updateDef)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);

            UpdateResult updateResult = collection.UpdateOne(filter, updateDef);
            return updateResult.ModifiedCount > 0 && updateResult.ModifiedCount == updateResult.MatchedCount ? true : false;
        }

        public bool UpdateOne<T>(string collectionName, FilterDefinition<T> filter, UpdateDefinition<T> updateDef)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);

            UpdateResult updateResult = collection.UpdateOne(filter, updateDef);
            return updateResult.ModifiedCount > 0 && updateResult.ModifiedCount == updateResult.MatchedCount ? true : false;
        }
        /// <summary>
        /// 更新操作
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <param name="updateDef"></param>
        /// <returns></returns>
        public bool UpdateMany<T>(string collectionName, Expression<Func<T, bool>> filter, UpdateDefinition<T> updateDef)
        {
            UpdateResult updateResult = UpdateManyWithResult(collectionName, filter, updateDef);
            return updateResult != null && updateResult.ModifiedCount > 0 && updateResult.ModifiedCount == updateResult.MatchedCount ? true : false;
        }
        /// <summary>
        /// 更新操作,返回更新数量
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <param name="updateDef">更新信息</param>
        /// <returns></returns>
        public UpdateResult UpdateManyWithResult<T>(string collectionName, Expression<Func<T, bool>> filter, UpdateDefinition<T> updateDef)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);

            return collection.UpdateMany(filter, updateDef);
        }

        #region 更新文档
        /// <summary>
        /// 更新子文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filterDefinition">条件</param>
        /// <param name="updateDef">更新信息</param>
        /// <returns></returns>
        public UpdateResult DocumentUpdate<T>(string collectionName, FilterDefinition<T> filterDefinition, UpdateDefinition<T> updateDef)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);

            return collection.UpdateMany(filterDefinition, updateDef);
        }
        #endregion
        #endregion

        #region 获取集合

        #region 分页列表
        /// <summary>
        /// 获取分页列表
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="pageIndex">当前页码</param>
        /// <param name="pageSize">每页数量</param>
        /// <param name="filter">条件</param>
        /// <param name="sort">排序信息</param>       
        /// <param name="total">总信息量</param>
        /// <param name="projection">输出的字段信息</param>
        /// <returns>返回集合</returns>
        public List<T> FindPageList<T>(string collectionName, int pageIndex, int pageSize, Expression<Func<T, bool>> filter, out int total, SortDefinition<T> sort = null, ProjectionDefinition<T> projection = null)
        {
            total = 0;
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            total = Convert.ToInt32(collection.Count(filter));

            if (projection == null)
            {
                var list = sort == null ?
                    collection.Find<T>(filter).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList() :
                    collection.Find<T>(filter).Sort(sort).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList();
                return list;
            }
            else
            {
                var list = sort == null ?
                    collection.Find<T>(filter).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).Project<T>(projection).ToList() :
                    collection.Find<T>(filter).Sort(sort).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).Project<T>(projection).ToList();
                return list;
            }
        }     

        #endregion
        public List<T> FindAllList<T>(string collectionName, SortDefinition<T> sort = null, ProjectionDefinition<T> projection = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            if (projection == null)
            {
                var list = sort == null ?
                    collection.Find<T>(x => true).ToList() :
                    collection.Find<T>(x => true).Sort(sort).ToList();
                return list;
            }
            else
            {
                var list = sort == null ?
                     collection.Find<T>(x => true).Project<T>(projection).ToList() :
                     collection.Find<T>(x => true).Sort(sort).Project<T>(projection).ToList();
                return list;
            }
        }

        /// <summary>
        /// 获得信息列表
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <param name="projection">输出字段信息</param>
        /// <returns>全部信息列表</returns>
        public List<T> List<T>(string collectionName, Expression<Func<T, bool>> filter, ProjectionDefinition<T> projection = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            if (projection == null)
            {
                var list = collection.Find<T>(filter == null ? x => true : filter).ToList();
                return list;
            }
            else
            {
                var list = collection.Find<T>(filter == null ? x => true : filter).Project<T>(projection).ToList();
                return list;
            }
        }

        /// <summary>
        /// 获取Top列表
        /// </summary>
        /// <param name="collectionName">集合名称</param>     
        /// <param name="size">选取数量</param>
        /// <param name="filter">条件</param>
        /// <param name="sort">排序信息</param>    
        /// <param name="projection">输出的字段信息</param>
        /// <returns>返回集合</returns>
        public List<T> TopList<T>(string collectionName, int size, Expression<Func<T, bool>> filter, SortDefinition<T> sort, ProjectionDefinition<T> projection = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            if (projection == null)
            {
                var list = sort == null ?
                    collection.Find<T>(filter).Limit(size).ToList() :
                    collection.Find<T>(filter).Sort(sort).Limit(size).ToList();
                return list;
            }
            else
            {
                var list = sort == null ?
                    collection.Find<T>(filter).Limit(size).Project<T>(projection).ToList() :
                    collection.Find<T>(filter).Sort(sort).Limit(size).Project<T>(projection).ToList();
                return list;
            }
        }
        #endregion

        #region 读取单条记录
        /// <summary>
        /// 读取单条记录
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <param name="projection">需要输出的字段信息</param>
        /// <returns>单条记录</returns>
        public T Single<T>(string collectionName, FilterDefinition<T> filter, FindOptions options = null,ProjectionDefinition<T> projection = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            var single = projection == null ? collection.Find(filter, options) : collection.Find(filter, options).Project<T>(projection);
            var list = single.ToList();
            if (list.Count > 0)
            {
                return list.First();
            }
            else
            {
                return default(T);
            }
        }
        #endregion

        #region 删除操作
        /// <summary>
        /// 删除操作
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <returns>删除的文档数</returns>
        public long Delete<T>(string collectionName, Expression<Func<T, bool>> filter)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            DeleteResult result = collection.DeleteMany(filter);
            return result.DeletedCount;
        }
        #endregion

        #region 统计
        /// <summary>
        /// 统计集合文档数
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter">条件</param>
        /// <returns>统计结果</returns>
        public long Count<T>(string collectionName, Expression<Func<T, bool>> filter)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            return collection.Count(filter);
        }

        /// <summary>
        /// 统计集合文档数
        /// </summary>
        /// <param name="collectionName">集合名称</param>
        /// <returns>统计结果</returns>
        public long Count(string collectionName)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<BsonDocument>(collectionName);
            return collection.Count(new BsonDocument());
        }
        #endregion

        #region 内嵌文档分页
        /// <summary>
        /// 内嵌文档分页
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filter"></param>
        /// <param name="projection"></param>
        /// <returns></returns>
        public T SubdocumentPageList<T>(string collectionName, Expression<Func<T, bool>> filter, ProjectionDefinition<T> projection)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);

            var item = collection.Find<T>(filter).Project<T>(projection);

            return item.Count()>0?item.First():default(T);
        }
        #endregion

        #region 文档是否存在
        /// <summary>
        /// 文档是否存在
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名称</param>
        /// <param name="filterDefinition">条件</param>
        /// <returns></returns>
        public bool ExistDocument<T>(string collectionName, FilterDefinition<T> filterDefinition)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);

            return collection.Find(filterDefinition).Count() > 0;
        }
        #endregion

        #region 聚合
        public List<T> Aggregate<T>(string collectionName, BsonDocument[] pipeline)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<T>(collectionName);
            AggregateOptions aop = new AggregateOptions();
            aop.AllowDiskUse = true;
            var list = collection.Aggregate<T>(pipeline, aop);
            return list.ToList();
        }

        public List<BsonDocument> Aggregate(string collectionName,string fieldUnwind,BsonDocument match,BsonDocument group)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<BsonDocument>(collectionName);

            var list = collection.Aggregate().Unwind(fieldUnwind).Match(match).Group(group);
            return list.ToList();
        }
        #endregion

        #region GridFS

        #region 上传操作
        public ObjectId UploadFromBytes(byte[] source, string fileName, GridFSUploadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return bucket.UploadFromBytes(fileName, source, options);
        }

        public async Task<ObjectId> UploadFromBytesAsync(byte[] source, string fileName, GridFSUploadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return await bucket.UploadFromBytesAsync(fileName, source, options);
        }

        public ObjectId UploadFromStream(Stream source, string fileName, GridFSUploadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return bucket.UploadFromStream(fileName, source, options);
        }
        public async Task<ObjectId> UploadFromStreamAsync(Stream source, string fileName, GridFSUploadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return await bucket.UploadFromStreamAsync(fileName, source, options);
        }

        public ObjectId UploadToStream(byte[] source, string fileName, GridFSUploadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            using (var stream = bucket.OpenUploadStream(fileName, options))
            {
                stream.Write(source, 0, source.Count());
                return stream.Id;
            }
        }

        public async Task<ObjectId> UploadToStreamAsync(byte[] source, string fileName, GridFSUploadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            using (var stream = await bucket.OpenUploadStreamAsync(fileName, options))
            {
                stream.Write(source, 0, source.Count());
                return stream.Id;
            }
        }

        #endregion

        #region 下载操作
        public byte[] DownloadAsBytes(ObjectId id, GridFSDownloadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return bucket.DownloadAsBytes(id, options);
        }

        public async Task<byte[]> DownloadAsBytesAsync(ObjectId id, GridFSDownloadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return await bucket.DownloadAsBytesAsync(id, options);
        }

        /// <summary>
        ///下载文件
        /// </summary>
        /// <param name="id">注意这个是files_id的值，而不是_id的值</param>
        /// <param name="destinationStream">文件流或者内存流</param>
        /// <param name="options"></param>
        public void DownloadToStream(ObjectId id, Stream destinationStream, GridFSDownloadOptions options = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            bucket.DownloadToStream(id, destinationStream, options);
        }

        public async void DownloadToStreamAsync(ObjectId id, Stream destinationStream, GridFSDownloadOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            await bucket.DownloadToStreamAsync(id, destinationStream, options);
        }

        public byte[] DownloadAsBytesByName(string fileName, GridFSDownloadByNameOptions options=null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            return bucket.DownloadAsBytesByName(fileName, options);
        }

        public void DownloadToStreamByName(string fileName, Stream destinationStream, GridFSDownloadByNameOptions options = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            bucket.DownloadToStreamByName(fileName, destinationStream, options);
        }

        #endregion

        #region 查找
        public List<GridFSFileInfo> Find(FilterDefinition<GridFSFileInfo> filter, GridFSFindOptions options = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            using (var cursor = bucket.Find(filter, options))
            {
                return cursor.ToList();
            }
        }
        #endregion

        #region 删除与重命名
        public void DeleteSingleFile(ObjectId id)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            bucket.Delete(id);
        }

        public void DropEntireBucket()
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            bucket.Drop();
        }

        /// <summary>
        /// 改变一个文件的名称
        /// </summary>
        /// <param name="id">files_id的值</param>
        /// <param name="newFileName">新的文件名</param>
        public void RenameSingleFile(ObjectId id,string newFileName)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            bucket.Rename(id, newFileName);
        }

        /// <summary>
        /// 改变所有版本文件的名称
        /// </summary>
        /// <param name="newFileName">新的文件名</param>
        /// <param name="filter">过滤器，用于筛选出文件</param>
        /// <param name="options"></param>
        public void RenameAllRevisions(string newFileName,FilterDefinition<GridFSFileInfo> filter, GridFSFindOptions options = null)
        {
            MongoClient client = GetClient(m_connectionStr);
            var db = client.GetDatabase(DatabaseName);
            var bucket = new GridFSBucket(db, BucksOptions);
            using (var cursor = bucket.Find(filter, options))
            {
                var files = cursor.ToList();

                foreach (var file in files)
                {
                    bucket.Rename(file.Id, newFileName);
                }
            }
        }

        #endregion
        #endregion

    }
}