using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp_MongoDB
{
    /// <summary>
    /// 模拟关系数据库的事务
    /// 任务描述：两个银行账户之间的转账操作
    /// { _id: 1, balance: 100, txns: [] },{ _id: 2, balance: 0, txns: [] }
    /// </summary>
    public class MimickingTransactionalBehavior
    {
        private string TransactionCollectionName = "TransactionCollection";
        private string AccountsCollectionName = "UserAccounts";
        private MongoDBService mongoDBService = new MongoDBService("mongodb://localhost:27017/MimiTransaction?maxPoolSize=100&minPoolSize=10",
               "MimiTransaction");

        public void Transfer(double amt, string source, string destination,TimeSpan maxTxnTime)
        {
            try
            {
                //准备转账
                TransactionDocument txn = PrepareTransfer(amt, source, destination);
                CommitTransfer(txn, maxTxnTime);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        //准备转账
        private TransactionDocument PrepareTransfer(double amt, string source, string destination)
        {
            string strGUID = System.Guid.NewGuid().ToString("N"); //类似e0a953c3ee6040eaa9fae2b667060e09
            //创建事务文档
            TransactionDocument tDoc = new TransactionDocument 
            {
                _id = strGUID, //这个应该是随机生成的一个串
                State ="new",
                Ts = DateTime.Now,
                Amt = amt,
                Src = source,
                Dst = destination
            };
            //将事务文档插入事务集合
            bool isSu = mongoDBService.Insert(TransactionCollectionName, tDoc);

            if(!isSu)
            {
                throw new Exception("构建事务文档失败！");
            }

            FilterDefinitionBuilder<Account> filterBuilder = Builders<Account>.Filter;
            //更新source账户
            FilterDefinition<Account> filterS = filterBuilder.Eq(m => m._id, source)&filterBuilder.Gte(m => m.Balance, amt);
            UpdateDefinition<Account> updateS = Builders<Account>.Update.Push(m => m.Txns, tDoc._id).Inc(m => m.Balance, -amt);
            UpdateResult updateResult = mongoDBService.DocumentUpdate(AccountsCollectionName, filterS, updateS);

            //检测更新是否成功
            bool isSuccess = updateResult.ModifiedCount > 0 && updateResult.ModifiedCount == updateResult.MatchedCount?
                true:false;
            if (!isSuccess)
            {
                mongoDBService.Delete<TransactionDocument>(TransactionCollectionName, m => m._id == tDoc._id);
                throw new Exception("更新source账户失败");
            }
            
            //更新destination账户
            FilterDefinition<Account> filterD = filterBuilder.Eq(m => m._id, destination);
            var updateD = Builders<Account>.Update.Push(m => m.Txns, tDoc._id).Inc(m => m.Balance, amt);
            UpdateResult updateResultD = mongoDBService.DocumentUpdate(AccountsCollectionName, filterD, updateD);
            bool isSuccessD = updateResultD.ModifiedCount > 0 && updateResultD.ModifiedCount == updateResultD.MatchedCount ?
                true : false;
            if (!isSuccessD)
            {
                throw new Exception("更新destination账户失败");
            }
            return tDoc;
        }

        //提交
        private void CommitTransfer(TransactionDocument txn, TimeSpan maxTxnTime)
        {
            DateTime now = DateTime.Now.ToUniversalTime();
            DateTime cutOff = now - maxTxnTime;

            //更新事务文档
            FilterDefinitionBuilder<TransactionDocument> filterBuilder = Builders<TransactionDocument>.Filter;
            FilterDefinition<TransactionDocument> filter1 = filterBuilder.Eq(m => m._id, txn._id);
            FilterDefinition<TransactionDocument> filter2 = filterBuilder.Gt(m => m.Ts, cutOff);
            FilterDefinition<TransactionDocument> filter = filterBuilder.And(new FilterDefinition<TransactionDocument>[] { filter1, filter2 });

            var update = Builders<TransactionDocument>.Update.Set(m => m.State, "commit");
            UpdateResult updateResult = mongoDBService.DocumentUpdate(TransactionCollectionName, filter, update);
            bool isSuccess = updateResult.ModifiedCount > 0 && updateResult.ModifiedCount == updateResult.MatchedCount ?
                true : false;
            if (!isSuccess)
            {
                throw new Exception("修改事务文档失败");
            }
            else
            {
                RetireTransaction(txn);
            }
        }

        //收回事务
        //事务收回方法也是幂等的
        private void RetireTransaction(TransactionDocument txn)
        {
            FilterDefinitionBuilder<Account> filterBuilder = Builders<Account>.Filter;
            FilterDefinition<Account> filter = filterBuilder.Eq(m => m._id, txn.Src);//source
            var update = Builders<Account>.Update.Pull(m => m.Txns, txn._id);
            mongoDBService.DocumentUpdate(AccountsCollectionName, filter, update);

            FilterDefinition<Account> filterD = filterBuilder.Eq(m => m._id, txn.Dst);//dest
            var updateD = Builders<Account>.Update.Pull(m => m.Txns, txn._id);
            mongoDBService.DocumentUpdate(AccountsCollectionName, filterD, updateD);

            mongoDBService.Delete<TransactionDocument>(TransactionCollectionName, m => m._id == txn._id);
        }

        //清理，这个方法应该定期执行
        //清理方法也是“幂等的”
        public void CleanupTransactions(TimeSpan maxTxnTime)
        {
            //原书中for txn in db.transaction.find({ 'state': 'commit' }, {'_id': 1}):中的1不对,去掉
            List<TransactionDocument> docCommitList = mongoDBService.List<TransactionDocument>(TransactionCollectionName, m => m.State == "commit");
            foreach (TransactionDocument tdoc in docCommitList)
            {
                RetireTransaction(tdoc);
            }

            DateTime now = DateTime.Now.ToUniversalTime();
            DateTime cutOff = now - maxTxnTime;
            //找出超时操作 回滚
            List<TransactionDocument> docRoolbackList = mongoDBService.List<TransactionDocument>(TransactionCollectionName, m => m.Ts.CompareTo(cutOff) < 0 && m.State == "new");
            foreach (TransactionDocument tdoc in docRoolbackList)
            {
                RollbackTransfer(tdoc);
            }
        }

        //回滚
        //这里的回滚方法是“幂等的”
        private void RollbackTransfer(TransactionDocument txn)
        {
            //恢复账户信息
            FilterDefinitionBuilder<Account> filterBuilder = Builders<Account>.Filter;
            FilterDefinition<Account> filter1 = filterBuilder.Eq(m => m._id, txn.Src);//source
            FilterDefinition<Account> filter2 = filterBuilder.Where(m => m.Txns.Contains(txn._id));
            FilterDefinition<Account> filter = filterBuilder.And(new FilterDefinition<Account>[]{filter1,filter2});
            var update = Builders<Account>.Update.Inc(m => m.Balance, txn.Amt).Pull(m =>m.Txns,txn._id);
            mongoDBService.DocumentUpdate(AccountsCollectionName, filter, update);

            FilterDefinition<Account> filterD1 = filterBuilder.Eq(m => m._id, txn.Dst);//dest
            FilterDefinition<Account> filterD2 = filterBuilder.Where(m => m.Txns.Contains(txn._id));
            FilterDefinition<Account> filterD = filterBuilder.And(new FilterDefinition<Account>[] { filterD1, filterD2 });
            var updateD = Builders<Account>.Update.Inc(m => m.Balance, -txn.Amt).Pull(m => m.Txns, txn._id);
            mongoDBService.DocumentUpdate(AccountsCollectionName, filterD, updateD);

            //删除事务文档
            mongoDBService.Delete<TransactionDocument>(TransactionCollectionName, m => m._id == txn._id);
        }

        public void TT()
        {
            Account ac = new Account 
            {
                _id = "3",
                Balance =100,
                Txns= new List<string>()
            };
            mongoDBService.Insert(AccountsCollectionName, ac);
        }
    }

    /// <summary>
    /// 事务文档
    /// 事务文档存在事务集合中
    /// </summary>
    public class TransactionDocument
    {
        public string _id { set; get; }
        public string State { set; get; }//状态
        public string Src { set; get; }//源
        public string Dst { set; get; }//目标

        public double Amt { set; get; }//金额
        public DateTime Ts { set; get; }//更新时间
    }

    /// <summary>
    /// 银行账号
    /// </summary>
    public class Account
    {
        public string _id { set; get; }
        public double Balance { set; get; }
        public List<string> Txns { set; get; }//事务文档的_id集合
    }
}
