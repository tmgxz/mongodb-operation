using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp_MongoDB
{
    /// <summary>
    ///注意！！！！！未完成，不可用
    /// </summary>
    public class MimickingTransactionalBehavior2
    {
        //事务文档集合
        private string TransactionCollectionName = "TransactionCollection";
        //账户集合
        private string AccountsCollectionName = "UserAccounts";
        private MongoDBService mongoDBService = new MongoDBService("mongodb://localhost:27017/TestDB?maxPoolSize=100&minPoolSize=10",
               "TestDB");
        //这一步失败一般是数据库或网络问题，一般若网络不稳定，通过重新执行几次应该可以解决问题
        //但是如果是后台系统问题，那么可能需要很长时间恢复
        /// <summary>
        /// 转账
        /// </summary>
        /// <param name="value">转账金额</param>
        /// <param name="source">源账户</param>
        /// <param name="destination">目标账户</param>
        public void Process(decimal value, string source, string destination)
        {
            //超时时间
            TimeSpan tSpan = new TimeSpan(0,0,100);
            //0 为参与事务的两个实体创建唯一的事务文档
            PrepareTransfer(value,source,destination);

            //1 找到状态为"initial"的事务文档
            TransactionDocumentP t2 = RetrieveTransaction();

            //2 将事务文档状态由“initial”更改为“pending”,超时跳出
            bool initial_pending = ReUpdateTransactionState(t2, "initial", "pending", tSpan);
            if (!initial_pending)
            {
                return;
            }
            //3 执行转账
            bool isSuccessAp = ApplyTransaction(t2, value, source, destination);
            if (!isSuccessAp)
            {
                //回滚
                RollbackOperations(t2, source, destination);
                return;
            }

            //4 将事务文档状态由“pending”更改为“applied”
            bool pending_applied = ReUpdateTransactionState(t2, "pending", "applied", tSpan);
            if (!pending_applied)
            {
                return;
            }

            //5 更新两个账户的待处理事务链表,移除事务标识,超时跳出
            bool update = UpdateAccount(t2, source, destination, tSpan);
            if (!update)
            {
                return;
            }

            //6 将事务文档状态由“applied”更改为“done”
            bool applied_done = ReUpdateTransactionState(t2, "applied", "done", tSpan);
            if (!applied_done)
            {
                return;
            }

            //7 将事务文档状态由“done”更改为“initial”
            bool done_initial = ReUpdateTransactionState(t2, "done", "initial", tSpan);
            if (!done_initial)
            {
                return;
            }
        }
        #region 主要流程
        //1 找到状态为"initial"的事务文档
        private TransactionDocumentP RetrieveTransaction()
        {
            FilterDefinitionBuilder<TransactionDocumentP> filterBuilder = Builders<TransactionDocumentP>.Filter;
            FilterDefinition<TransactionDocumentP> filter = filterBuilder.Eq(doc => doc.State, "initial");

            return mongoDBService.Single(TransactionCollectionName, filter);
        }
        //3 执行转账
        private bool ApplyTransaction(TransactionDocumentP t, decimal value, string source, string destination)
        {
            FilterDefinitionBuilder<AccountP> filterBuilderS = Builders<AccountP>.Filter;
            FilterDefinition<AccountP> filterS1 = filterBuilderS.Eq(doc => doc._id, source);
            var updateS = Builders<AccountP>.Update.Inc(m => m.Balance, -value).Push(m => m.PendingTransactions, t._id);
            UpdateResult updateResultS = mongoDBService.DocumentUpdate(AccountsCollectionName, filterS1, updateS);

            bool isSuss = updateResultS.ModifiedCount > 0 && updateResultS.ModifiedCount == updateResultS.MatchedCount;
            if(isSuss)
            {
                FilterDefinitionBuilder<AccountP> filterBuilderD = Builders<AccountP>.Filter;
                FilterDefinition<AccountP> filterD1 = filterBuilderD.Eq(doc => doc._id, destination);
                var updateD = Builders<AccountP>.Update.Inc(m => m.Balance, value).Push(m => m.PendingTransactions, t._id);
                UpdateResult updateResultD = mongoDBService.DocumentUpdate(AccountsCollectionName, filterD1, updateD);
                isSuss = updateResultD.ModifiedCount > 0 && updateResultD.ModifiedCount == updateResultD.MatchedCount;
            }

            return isSuss;
        }

        //5 更新两个账户的待处理事务链表,移除事务标识
        private bool UpdateAccount(TransactionDocumentP t, string source, string destination, TimeSpan maxTxnTime)
        {
            FilterDefinitionBuilder<AccountP> filterBuilderS = Builders<AccountP>.Filter;
            FilterDefinition<AccountP> filterS = filterBuilderS.Eq(doc => doc._id, source);
            var updateS = Builders<AccountP>.Update.Pull(doc => doc.PendingTransactions, t._id);
            bool isSucc = mongoDBService.UpdateOne(AccountsCollectionName, filterS, updateS);
            while (true)
            {
                if (isSucc) break;
                bool timeOut = CheckTimeOut(t, maxTxnTime);
                if (timeOut) break;
                isSucc = mongoDBService.UpdateOne(AccountsCollectionName, filterS, updateS);
            }
            if (!isSucc)
            {
                return isSucc;
            }

            FilterDefinitionBuilder<AccountP> filterBuilderD = Builders<AccountP>.Filter;
            FilterDefinition<AccountP> filterD = filterBuilderD.Eq(doc => doc._id, destination);
            var updateD = Builders<AccountP>.Update.Pull(doc => doc.PendingTransactions, t._id);
            isSucc = mongoDBService.UpdateOne(AccountsCollectionName, filterD, updateD);
            while (true)
            {
                if (isSucc) break;
                bool timeOut = CheckTimeOut(t, maxTxnTime);
                if (timeOut) break;
                isSucc = mongoDBService.UpdateOne(AccountsCollectionName, filterD, updateD);
            }
            return isSucc;
        }
        #endregion

        #region 辅助处理
        //检测超时
        private bool CheckTimeOut(TransactionDocumentP t, TimeSpan maxTxnTime)
        {
            DateTime cutOff = DateTime.Now - maxTxnTime;
            FilterDefinitionBuilder<TransactionDocumentP> filterBuilder = Builders<TransactionDocumentP>.Filter;
            FilterDefinition<TransactionDocumentP> filter = filterBuilder.Lt(doc => doc.LastModified, cutOff);
            var tranDoc = mongoDBService.Single(TransactionCollectionName, filter);
            return tranDoc == null ? true : false;
        }

        //重复执行更新状态操作，超时跳出
        private bool ReUpdateTransactionState(TransactionDocumentP t, string oldState, string newState,TimeSpan maxTxnTime)
        {
            bool isSucc = UpdateTransactionState(t, oldState, newState);
            while (true)
            {
                if (isSucc) break;
                bool timeOut = CheckTimeOut(t, maxTxnTime);
                if (timeOut) break;
                isSucc = UpdateTransactionState(t, oldState, newState);
            }
            return isSucc;
        }
        //超时解决不了的问题，只能通过定时执行方法清除，恢复
        private void RecoveryOperations(TransactionDocumentP t,string state,DateTime maxTxnTime,decimal value, string source, string destination)
        {
            DateTime now = DateTime.Now;
            DateTime cutOff = DateTime.Parse((now - maxTxnTime).ToString());
            FilterDefinitionBuilder<TransactionDocumentP> filterBuilder = Builders<TransactionDocumentP>.Filter;
            FilterDefinition<TransactionDocumentP> filter1 = filterBuilder.Eq(doc => doc.State, state);
            FilterDefinition<TransactionDocumentP> filter2 = filterBuilder.Lt(doc => doc.LastModified, cutOff);
            FilterDefinition<TransactionDocumentP> filter = Builders<TransactionDocumentP>.Filter.And(new FilterDefinition<TransactionDocumentP>[] { filter1, filter2 });
        }

        //幂等操作
        private void RollbackOperations(TransactionDocumentP t,string source, string destination)
        {
            //1 将事务文档状态由pending更新为canceling.
            ReUpdateTransactionState(t, "pending", "canceling", new TimeSpan(0,0,100));
            
            //2 账户余额回滚.
            FilterDefinitionBuilder<AccountP> filterBuilderS = Builders<AccountP>.Filter;
            FilterDefinition<AccountP> filterS1 = filterBuilderS.Eq(doc => doc._id, t.Source);//source
            FilterDefinition<AccountP> filterS2 = filterBuilderS.Where(doc => doc.PendingTransactions.Contains(t._id));
            FilterDefinition<AccountP> filterS = filterBuilderS.And(new FilterDefinition<AccountP>[] { filterS1, filterS2 });
            var updateS = Builders<AccountP>.Update.Inc(m => m.Balance, t.Value).Pull(m => m.PendingTransactions, t._id);
            bool isSuccess = mongoDBService.UpdateOne(AccountsCollectionName, filterS, updateS);

            if(isSuccess)
            {
                FilterDefinitionBuilder<AccountP> filterBuilderD = Builders<AccountP>.Filter;
                FilterDefinition<AccountP> filterD1 = filterBuilderD.Eq(doc => doc._id, t.Destination);//source
                FilterDefinition<AccountP> filterD2 = filterBuilderD.Where(doc => doc.PendingTransactions.Contains(t._id));
                FilterDefinition<AccountP> filterD = filterBuilderD.And(new FilterDefinition<AccountP>[] { filterD1, filterD2 });
                var updateD = Builders<AccountP>.Update.Inc(m => m.Balance, -t.Value).Pull(m => m.PendingTransactions, t._id);
                isSuccess = mongoDBService.UpdateOne(AccountsCollectionName, filterD, updateD);
            }

            if (isSuccess)
            {
                //3 将事务文档状态由canceling更新为cancelled.
                UpdateTransactionState(t, "canceling", "cancelled");
            }
        }
        
        private bool UpdateTransactionState(TransactionDocumentP t, string oldState, string newState)
        {
            if (t == null)
            {
                return false;
            }
            FilterDefinitionBuilder<TransactionDocumentP> filterBuilder = Builders<TransactionDocumentP>.Filter;
            FilterDefinition<TransactionDocumentP> filter1 = filterBuilder.Eq(doc => doc._id, t._id);
            FilterDefinition<TransactionDocumentP> filter2 = filterBuilder.Eq(doc => doc.State, oldState);
            FilterDefinition<TransactionDocumentP> filter = filterBuilder.And(new FilterDefinition<TransactionDocumentP>[] { filter1, filter2 });

            var update = Builders<TransactionDocumentP>.Update.Set(m => m.State, newState).Set(m =>m.LastModified,DateTime.Now);
            UpdateResult updateResult = mongoDBService.DocumentUpdate(TransactionCollectionName, filter, update);

            return  updateResult.ModifiedCount > 0 && updateResult.ModifiedCount == updateResult.MatchedCount;
        }

        //创建事务文档
        private void PrepareTransfer(decimal value, string source, string destination)
        {
            //创建事务文档
            TransactionDocumentP tDoc = new TransactionDocumentP
            {
                _id = string.Format("{0}For{1}", source, destination),
                State = "initial",
                LastModified = DateTime.Now,
                Value = value,
                Source = source,
                Destination = destination
            };
            FilterDefinitionBuilder<TransactionDocumentP> filterBuilder = Builders<TransactionDocumentP>.Filter;
            FilterDefinition<TransactionDocumentP> filter1 = filterBuilder.Eq(doc => doc._id, tDoc._id);
            if (mongoDBService.ExistDocument(TransactionCollectionName, filter1))
            {
                return;
            }
            //将事务文档插入事务集合
            mongoDBService.Insert(TransactionCollectionName, tDoc);
        }
        #endregion
    }
    /// <summary>
    /// 事务文档
    /// 事务文档存储在事务集合中
    /// </summary>
    public class TransactionDocumentP
    {
        public object _id { set; get; }
        //原账户
        public string Source { set; get; }
        //目标账户
        public string Destination { set; get; }
        //转账金额
        [BsonRepresentation(BsonType.Decimal128)]
        public decimal Value { set; get; }
        //执行状态（初始化initial, 执行操作pending, 完成操作applied, 事务结束done, 正在取消操作canceling, 完成取消canceled）
        public string State { set; get; }
        //最后修改日期
        public DateTime LastModified { set; get; }
    }
    /// <summary>
    /// 银行账号
    /// </summary>
    public class AccountP
    {
        /// <summary>
        /// 账号
        /// </summary>
        public string _id { set; get; }
        /// <summary>
        /// 账户余额
        /// </summary>
        [BsonRepresentation(BsonType.Decimal128)]
        public decimal Balance { set; get; }
        /// <summary>
        /// 待处理事务链表
        /// </summary>
        public List<string> PendingTransactions { set; get; }
    }

    public class Show
    {
        private static string TransactionCollectionName = "TransactionCollection";
        private static string AccountsCollectionName = "UserAccounts";
        private static MongoDBService mongoDBService = new MongoDBService("mongodb://localhost:27017/PubSub?maxPoolSize=100&minPoolSize=10",
               "PubSub");

        
        private static void CreateAccount()
        {
            AccountP accA = new AccountP
            {
                _id = "A001",
                Balance = 2010,
                PendingTransactions = new List<string>()
            };
            AccountP accB = new AccountP
            {
                _id = "B001",
                Balance = 2010,
                PendingTransactions = new List<string>()
            };
            mongoDBService.Insert(AccountsCollectionName, accA);
            mongoDBService.Insert(AccountsCollectionName, accB);
        }

        public static void Exec()
        {
            CreateAccount();
            MimickingTransactionalBehavior2 mtb = new MimickingTransactionalBehavior2();
            mtb.Process(100, "A001", "B001");
        }

    }
}
