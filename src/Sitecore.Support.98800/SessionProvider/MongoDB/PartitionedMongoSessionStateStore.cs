using System;
using System.Collections.Generic;
using System.Globalization;
using System.Web.SessionState;
using MongoDB.Driver;
using Sitecore.Diagnostics;

namespace Sitecore.Support.SessionProvider.MongoDB
{
  internal class PartitionedMongoSessionStateStore : IMongoSessionStateStore
  {
    private const string DEFAULT_DATABASE_NAME = "sessions";
    private const int SESSION_END_PREFETCH_BATCH = 16;



    private readonly Random m_Random = new Random();
    private readonly MongoSessionStateStore[] m_Partitions;




    internal PartitionedMongoSessionStateStore([NotNull] string connectionString, int partitions, bool compress)
    {
      Debug.ArgumentNotNull(connectionString, "connectionString");
      Debug.Assert(partitions > 0, "Invalid number of partitions.");

      this.m_Partitions = new MongoSessionStateStore[partitions];

      MongoUrl url = new MongoUrl(connectionString);
      string databaseNameBase = (url.DatabaseName ?? DEFAULT_DATABASE_NAME);

      for (int i = 0; i < this.m_Partitions.Length; i++)
      {
        string databaseName = string.Format(CultureInfo.InvariantCulture, "{0}_{1:D3}", databaseNameBase, i);

        this.m_Partitions[i] = new MongoSessionStateStore(connectionString, databaseName, compress);
      }
    }



    public void Initialize()
    {
      for (int i = 0; i < this.m_Partitions.Length; i++)
      {
        this.m_Partitions[i].Initialize();
      }
    }



    public SessionStateStoreData GetItem(string application, string id, out Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      MongoSessionStateStore partition = this.GetPartition(id);
      SessionStateStoreData result = partition.GetItem(application, id, out lockCookie, out flags);

      return result;
    }



    public SessionStateStoreData GetItemExclusive(string application, string id, Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      MongoSessionStateStore partition = this.GetPartition(id);
      SessionStateStoreData result = partition.GetItemExclusive(application, id, lockCookie, out flags);

      return result;
    }



    public void ReleaseItem(string application, string id, string lockCookie)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      MongoSessionStateStore partition = this.GetPartition(id);

      partition.ReleaseItem(application, id, lockCookie);
    }



    public void UpdateAndReleaseItem(string application, string id, string lockCookie, SessionStateActions action, SessionStateStoreData sessionState)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");
      Assert.ArgumentNotNull(sessionState, "sessionState");

      MongoSessionStateStore partition = this.GetPartition(id);

      partition.UpdateAndReleaseItem(application, id, lockCookie, action, sessionState);
    }



    public Sitecore.SessionProvider.SessionStateLockCookie GetItemLock(string application, string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      MongoSessionStateStore partition = this.GetPartition(id);
      Sitecore.SessionProvider.SessionStateLockCookie result = partition.GetItemLock(application, id);

      return result;
    }



    public void InsertItem(string application, string id, int flags, SessionStateStoreData sessionState)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(sessionState, "sessionState");

      MongoSessionStateStore partition = this.GetPartition(id);

      partition.InsertItem(application, id, flags, sessionState);
    }



    public void RemoveItem(string application, string id, string lockCookie)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      MongoSessionStateStore partition = this.GetPartition(id);

      partition.RemoveItem(application, id, lockCookie);
    }



    public void UpdateItemExpiration(string application, string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      MongoSessionStateStore partition = this.GetPartition(id);

      partition.UpdateItemExpiration(application, id);
    }

    /// <summary>
    ///   Attempts to lock and load an expired session state store item.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="signalTime">
    ///   The date and time the signal for processing expired sessions was generated, expressed as UTC.
    ///   <c>DateTime.Kind</c> should be <c>DateTimeKind.Utc</c>.
    /// </param>
    /// <param name="lockCookie">
    ///   A <see cref="SessionStateLockCookie"/> object that is the lock to set.
    /// </param>
    /// <returns>
    ///   A <see cref="SessionStateStoreData"/> object loaded from the database if the session state store entry was
    ///   found and successfully locked; otherwise, <c>null</c>.
    /// </returns>
    /// <remarks>
    ///   This method returns <c>null</c> if the session state entry does not exist or if the entry is already locked.
    /// </remarks>
    public SessionStateStoreData GetExpiredItemExclusive(string application, DateTime signalTime, Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      id = null;

      SessionStateStoreData result = null;
      int attempts = this.m_Partitions.Length;
      int partition = this.m_Random.Next(this.m_Partitions.Length);

      IList<MongoSessionStateStore.SessionEndCandidate> candidates = new List<MongoSessionStateStore.SessionEndCandidate>(SESSION_END_PREFETCH_BATCH);

      do
      {
        this.m_Partitions[partition].GetSessionEndCandidates(application, signalTime, SESSION_END_PREFETCH_BATCH, candidates);

        if (candidates.Count > 0)
        {
          do
          {
            int index = this.m_Random.Next(candidates.Count);
            MongoSessionStateStore.SessionEndCandidate candidate = candidates[index];
            candidates.RemoveAt(index);

            SessionStateStoreData item = this.m_Partitions[partition].GetExpiredItemExclusive(application, candidate, lockCookie);

            if (item != null)
            {
              id = candidate.Id;
              result = item;
            }
          }
          while ((result == null) && (candidates.Count > 0));
        }
        else
        {
          attempts -= 1;
          partition += 1;

          if (partition == this.m_Partitions.Length)
          {
            partition = 0;
          }
        }
      }
      while ((result == null) && (attempts > 0));

      return result;
    }



    [NotNull]
    private MongoSessionStateStore GetPartition([NotNull] string id)
    {
      Debug.ArgumentNotNull(id, "id");

      int index = this.GetPartitionIndex(this.m_Partitions.Length, id);
      MongoSessionStateStore result = this.m_Partitions[index];

      return result;
    }



    private int GetPartitionIndex(int partitions, [NotNull] string id)
    {
      Debug.ArgumentNotNull(id, "id");

      double hash = id.GetHashCode();

      double maximum = (((double)int.MaxValue) - int.MinValue);
      double length = (maximum / partitions);
      double current = (hash - int.MinValue);
      double partition = Math.Floor(current / length);

      int result = ((int)partition);

      return result;
    }
  }
}