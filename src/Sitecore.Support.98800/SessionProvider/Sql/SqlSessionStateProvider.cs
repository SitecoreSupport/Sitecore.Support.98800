using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.Web;
using System.Web.SessionState;
using Sitecore.Diagnostics;
using Sitecore.SessionProvider;
using Sitecore.SessionProvider.Helpers;

namespace Sitecore.Support.SessionProvider.Sql
{
  public class SqlSessionStateProvider : SitecoreSessionStateStoreProvider
  {
    private Guid m_ApplicationId;

    private SqlSessionStateStore m_Store;

    /// <summary>
    /// List to store all the instances of this class
    /// </summary>
    private static readonly List<SqlSessionStateProvider> SqlSessionStateProvidersList = new List<SqlSessionStateProvider>();

    /// <summary>
    /// Lock object
    /// </summary>
    private static readonly object ListSyncRoot = new object();

    static SqlSessionStateProvider()
    {
      Trace.WriteLine("SQL Session State Provider is initializing.", "SqlSessionStateProvider");
    }

    private Guid ApplicationId
    {
      get
      {
        return this.m_ApplicationId;
      }
    }

    [NotNull]
    private SqlSessionStateStore Store
    {
      get
      {
        System.Diagnostics.Debug.Assert(null != this.m_Store);

        return this.m_Store;
      }
    }

    public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");

      const int flags = (int)SessionStateActions.InitializeItem;
      var sessionItems = new SessionStateItemCollection();
      var staticObjects = new HttpStaticObjectsCollection();
      var sessionState = new SessionStateStoreData(sessionItems, staticObjects, timeout);

      this.m_Store.InsertItem(this.ApplicationId, id, flags, sessionState);
    }

    public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");

      locked = false;
      lockAge = TimeSpan.Zero;
      lockId = null;
      actions = SessionStateActions.None;

      int flags = 0;
      SessionStateLockCookie lockCookie = null;

      SessionStateStoreData result = this.Store.GetItem(this.ApplicationId, id, out lockCookie, out flags);

      actions = (SessionStateActions)flags;

      if (null != lockCookie)
      {
        locked = true;
        lockId = lockCookie.Id;
        lockAge = DateTime.UtcNow - lockCookie.Timestamp;
      }

      return result;
    }

    public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");

      // Set the initial values of the out parameters. These are the values to be returned if the requested session
      // state store item does not exist.
      lockAge = TimeSpan.Zero;
      actions = SessionStateActions.None;

      int flags = 0;
      SessionStateLockCookie existingLockCookie = null;
      SessionStateLockCookie acquiredLockCookie = SessionStateLockCookie.Generate(DateTime.UtcNow);

      SessionStateStoreData result = this.Store.GetItemExclusive(this.ApplicationId, id, acquiredLockCookie, out existingLockCookie, out flags);

      if (existingLockCookie != null)
      {
        locked = true;
        lockAge = DateTime.UtcNow - existingLockCookie.Timestamp;
        lockId = existingLockCookie.Id; // ??
      }
      else
      {
        locked = false;
        lockId = acquiredLockCookie.Id; // ??
        actions = (SessionStateActions)flags;
      }

      return result;
    }

    public override void Initialize(string name, NameValueCollection config)
    {
      Assert.ArgumentNotNull(name, "name");
      Assert.ArgumentNotNull(config, "config");

      base.Initialize(name, config);

      var configuration = new ConfigReader(config, name);

      string applicationName = configuration.GetString("sessionType", true);
      string connectionName = configuration.GetString("connectionStringName", false);
      string connectionString = ConfigurationManager.ConnectionStrings[connectionName].ConnectionString;

      bool compression = configuration.GetBool("compression", false);

      this.m_Store = new SqlSessionStateStore(connectionString, compression);
      this.m_ApplicationId = this.m_Store.GetApplicationIdentifier(applicationName);
      lock (ListSyncRoot)
      {
        SqlSessionStateProvidersList.Add(this);
      }
      this.CanStartTimer = this.isTimerOffForAllInstance;
    }

    private bool isTimerOffForAllInstance()
    {
      lock (ListSyncRoot)
      {
        foreach (var sqlSessionStateProvider in SqlSessionStateProvidersList)
        {
          if (sqlSessionStateProvider.ApplicationId == this.ApplicationId &&
              sqlSessionStateProvider.TriedToStartTimer &&
              sqlSessionStateProvider.TimerEnabled)
            return false;
        }
        return true;
      }
    }

    /// <summary>
    /// Releases managed and unmanaged resources.
    /// </summary>
    public override void Dispose()
    {
      lock (ListSyncRoot)
      {
        SqlSessionStateProvidersList.Remove(this);
        if (this.TimerEnabled && SqlSessionStateProvidersList.Count > 0)
        {
          foreach (var sqlSessionStateProvider in SqlSessionStateProvidersList)
          {
            if (sqlSessionStateProvider.ApplicationId == this.ApplicationId &&
                sqlSessionStateProvider.TriedToStartTimer)
            {
              sqlSessionStateProvider.StartTimer();
              break;
            }
          }
        }
      }
      base.Dispose();
    }

    public override void ReleaseItemExclusive([NotNull] HttpContext context, string id, object lockId)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockId, "lockId");

      string lockCookie = System.Convert.ToString(lockId);

      this.Store.ReleaseItem(this.ApplicationId, id, lockCookie);
    }

    public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockId, "lockId");

      string lockCookie = System.Convert.ToString(lockId);

      try
      {
        this.ExecuteSessionEnd(id, item);
      }
      finally
      {
        this.Store.RemoveItem(this.ApplicationId, id, lockCookie);
      }
    }

    public override void ResetItemTimeout(HttpContext context, string id)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");

      this.Store.UpdateItemExpiration(this.ApplicationId, id);
    }

    public override void SetAndReleaseItemExclusive([NotNull] HttpContext context, string id, SessionStateStoreData sessionState, object lockId, bool newItem)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(id, "id");

      if (newItem)
      {
        const int flags = (int)SessionStateActions.None;
        this.Store.InsertItem(this.ApplicationId, id, flags, sessionState);
      }
      else
      {
        string lockCookie = System.Convert.ToString(lockId);
        this.Store.UpdateAndReleaseItem(this.ApplicationId, id, lockCookie, SessionStateActions.None, sessionState);
      }
    }

    protected override SessionStateStoreData GetExpiredItemExclusive(DateTime signalTime, SessionStateLockCookie lockCookie, out string id)
    {
      return this.Store.GetExpiredItemExclusive(this.ApplicationId, lockCookie, out id);
    }

    protected override void RemoveItem(string id, string lockCookie)
    {
      this.Store.RemoveItem(this.ApplicationId, id, lockCookie);
    }
  }
}