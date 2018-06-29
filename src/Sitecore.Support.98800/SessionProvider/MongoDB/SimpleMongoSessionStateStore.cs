using System;
using System.Collections.Generic;
using System.Web.SessionState;
using Sitecore.Diagnostics;

namespace Sitecore.Support.SessionProvider.MongoDB
{
  internal sealed class SimpleMongoSessionStateStore : IMongoSessionStateStore
  {
    private const int SESSION_END_PREFETCH_BATCH = 16;



    private readonly Random m_Random = new Random();
    private readonly MongoSessionStateStore m_Store;



    internal SimpleMongoSessionStateStore([NotNull] string connectionString, bool compress)
    {
      Debug.ArgumentNotNull(connectionString, "connectionString");

      this.m_Store = new MongoSessionStateStore(connectionString, compress);
    }



    public void Initialize()
    {
      this.m_Store.Initialize();
    }



    public SessionStateStoreData GetItem(string application, string id, out Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      SessionStateStoreData result = this.m_Store.GetItem(application, id, out lockCookie, out flags);

      return result;
    }



    public SessionStateStoreData GetItemExclusive(string application, string id, Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      SessionStateStoreData result = this.m_Store.GetItemExclusive(application, id, lockCookie, out flags);

      return result;
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
    ///   A <see cref="Sitecore.SessionProvider.SessionStateLockCookie"/> object that is the lock to set.
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
      IList<MongoSessionStateStore.SessionEndCandidate> candidates = new List<MongoSessionStateStore.SessionEndCandidate>(SESSION_END_PREFETCH_BATCH);

      while (true)
      {
        if (candidates.Count == 0)
        {
          int count = this.m_Store.GetSessionEndCandidates(application, signalTime, SESSION_END_PREFETCH_BATCH, candidates);

          if (count == 0)
          {
            break;
          }
        }

        int index = this.m_Random.Next(candidates.Count);
        MongoSessionStateStore.SessionEndCandidate candidate = candidates[index];
        candidates.RemoveAt(index);

        SessionStateStoreData item = this.m_Store.GetExpiredItemExclusive(application, candidate, lockCookie);

        if (item != null)
        {
          id = candidate.Id;
          result = item;

          break;
        }
      }

      return result;
    }



    public Sitecore.SessionProvider.SessionStateLockCookie GetItemLock(string application, string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      Sitecore.SessionProvider.SessionStateLockCookie result = this.m_Store.GetItemLock(application, id);

      return result;
    }



    public void UpdateAndReleaseItem(string application, string id, string lockCookie, SessionStateActions action, SessionStateStoreData sessionState)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");
      Assert.ArgumentNotNull(sessionState, "sessionState");

      this.m_Store.UpdateAndReleaseItem(application, id, lockCookie, action, sessionState);
    }



    public void ReleaseItem(string application, string id, string lockCookie)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      this.m_Store.ReleaseItem(application, id, lockCookie);
    }



    public void RemoveItem(string application, string id, string lockCookie)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      this.m_Store.RemoveItem(application, id, lockCookie);
    }



    public void InsertItem(string application, string id, int flags, SessionStateStoreData sessionState)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(sessionState, "sessionState");

      this.m_Store.InsertItem(application, id, flags, sessionState);
    }



    public void UpdateItemExpiration(string application, string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      this.m_Store.UpdateItemExpiration(application, id);
    }
  }
}