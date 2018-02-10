using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp_MongoDB
{
    public class GridFSOperation
    {
        private MongoDBService mongoDBService;
        public GridFSOperation(MongoDBService mongoDBService)
        {
            this.mongoDBService = mongoDBService;
            this.mongoDBService.BucksOptions = new GridFSBucketOptions 
            {
                BucketName = "firstbucket",
                ChunkSizeBytes =102400,
            };
        }

        public void UpLoad()
        {
            //读文件流，上传
            using (FileStream sr = new FileStream(@"D:\敏感词检测算法.docx", FileMode.Open))
            {

                mongoDBService.UploadFromStream(sr, "gridfsTest", null);
            }

            //字节数组，上传
            //string str = "测试上传字节数组";
            //byte[] b = Encoding.Default.GetBytes(str);
            //mongoDBService.UploadFromBytes(b, "gridfsTest");

            //mongoDBService.UploadToStream(b, "gridfsTest");
        }

        public void DownLoad()
        {
            using(FileStream fs = new FileStream(@"D:\gridfsDownload.jpg", FileMode.Create))
            {
                mongoDBService.DownloadToStream(new ObjectId("5a61a113379d582f14e750a3"), fs);
            }

            var b = mongoDBService.DownloadAsBytes(new ObjectId("5a61b103379d5829f89d0688"));
            Console.WriteLine(Encoding.Default.GetString(b));

            //异步方法
            var bs = mongoDBService.DownloadAsBytesAsync(new ObjectId("5a61b103379d5829f89d0688"));
            bs.Wait();
            Console.WriteLine("异步的方法，获得的字符串为：" + Encoding.Default.GetString(bs.Result));


            //通过文件名下载
            //当多个文件的文件名相同时，可以通过指定版本来选择下载哪一个文件，默认的是-1（最新上传的版本）
            //0,原始版
            //1,第一版
            //2,第二版
            //.....
            //-1,最新版
            //-2,次新版
            //var bN = mongoDBService.DownloadAsBytesByName("gridfsTest");
            //Console.WriteLine("通过文件名，获得的字符串为：" + Encoding.Default.GetString(bN));

            //指定版本
            var options = new GridFSDownloadByNameOptions
            {
                Revision = 0
            };
            using (FileStream fs = new FileStream(@"D:\gridfsDownloadByName.jpg", FileMode.Create))
            {
                mongoDBService.DownloadToStreamByName("gridfsTest", fs,options);
            }
        }

        public void Find()
        {
            //指定版本
            //var options = new GridFSFindOptions
            //{
            //    BatchSize
            //};

            var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, "gridfsTestNewName1");
            var info = mongoDBService.Find(filter);
        }
        public void ReName()
        {
            //改一个文件
            mongoDBService.RenameSingleFile(new ObjectId("5a61b103379d5829f89d0688"), "gridfsTestNewName");

            //全改
            var filter = Builders<GridFSFileInfo>.Filter.Eq(x => x.Filename, "gridfsTest");
            mongoDBService.RenameAllRevisions("gridfsTestNewName1", filter);
        }

        public void Delete()
        {
            //删除指定的一个文件
            mongoDBService.DeleteSingleFile(new ObjectId("5a61a113379d582f14e750a3"));

            //删除整个chunks
            mongoDBService.DropEntireBucket();
        }
    }
}
