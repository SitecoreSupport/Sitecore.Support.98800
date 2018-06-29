using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.SessionState;
using Sitecore.SessionProvider.Helpers;

namespace Sitecore.Support.SessionProvider.MongoDB
{
  /// <summary>
  ///   ASP.NET Session State Provider with Session End support for MongoDB.
  /// </summary>
  /// <remarks>
  /// <para>
  /// Some Notes: According to the Microsoft official recommendations, a Session-State Store Provider should implement a 
  /// mechanism to delete expired session data. The class Sitecore.SessionProvider.SitecoreSessionStateStoreProvider (it's 
  /// a base abstract class for the actual provider type) maintains a timer (System.Timers.Timer) to delete that expired session data. 
  /// By default, each 2 seconds the timer runs a special handler OnProcessExpiredItems to clean up the expired session data. 
  /// The handler, in turn, executes the SQL stored procedure GetExpiredItemExclusive against the session database. And 
  /// everything goes smoothly until you have only one instance of the Session-State Store Provider. However, as you 
  /// probably know, ASP.NET may create additional workers when the existing ones are not enough to handle the incoming requests. 
  /// For each separate worker, the SessionStateModule initializes a separate instance of  Session-State Store Provider. 
  /// At some point, a number of such providers execute the stored procedure in a number of concurrent transactions in separate SQL connections. 
  /// As a result, you get a high number of connections and heavy load on your database server.
  /// </para>
  /// <para>
  /// In order to prevent multiple executions of the OnProcessExpiredItems handler we have maintained a static list of all 
  /// the instances and used dependency injection (CanStartTimer delegate) to control the start of the timer inside the base
  /// class from the child class and a graceful Dispose method to sync timers from multiple instances. The main idea is to 
  /// guarantee that only one instance of the session state provider may execute the handler at any particular point in time.
  /// And while one handler works another can't be executed.
  /// </para>
  /// </remarks>
  public class MongoSessionStateProvider : SitecoreSessionStateStoreProvider
  {
    private static int initialized;

    private IMongoSessionStateStore m_Store;
    private string m_ApplicationName;

    /// <summary>
    /// List to store all the instances of this class
    /// </summary>
    private static readonly List<MongoSessionStateProvider> MongoSessionStateProvidersList = new List<MongoSessionStateProvider>();

    /// <summary>
    /// Lock object
    /// </summary>
    private static readonly object ListSyncRoot = new object();

    /// <summary>
    ///   Gets a <see cref="IMongoSessionStateStore"/> object that provides methods for accessing the session database.
    /// </summary>
    /// <value>
    ///   A <see cref="IMongoSessionStateStore"/> object that provides methods for accessing the session database.
    /// </value>
    [NotNull]
    private IMongoSessionStateStore Store
    {
      get
      {
        Debug.Assert(null != this.m_Store);

        return this.m_Store;
      }
    }

    /// <summary>
    /// Initializes static members of the <see cref="MongoSessionStateProvider"/> class.
    /// </summary>
    static MongoSessionStateProvider()
    {
      Diagnostics.Log.Info("MongoDB Session State Provider is initializing.", "MongoSessionStateProvider");
    }

    /// <summary>
    ///   Initializes the current provider.
    /// </summary>
    /// <param name="name">
    ///   The friendly name of the provider.
    /// </param>
    /// <param name="config">
    ///   A collection of the name/value pairs representing the provider-specific attributes specified in the
    ///   configuration for this provider.
    /// </param>
    /// <exception cref="Sitecore.Exceptions.ConfigurationException">The polling interval specified is too small or too high.</exception>
    public override void Initialize(string name, NameValueCollection config)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(name, "name");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(config, "config");

      base.Initialize(name, config);

      ConfigReader configuration = new ConfigReader(config, name);

      string connectionName = configuration.GetString("connectionStringName", false);
      string connectionString = ConfigurationManager.ConnectionStrings[connectionName].ConnectionString;

      this.m_ApplicationName = configuration.GetString("sessionType", false);
      bool compression = configuration.GetBool("compression", false);

      int partitions = configuration.GetInt32("partitions", 0);

      if (partitions < 0)
      {
        throw new Sitecore.Exceptions.ConfigurationException("The number of partitions specified is not valid.");
      }

      if (partitions > 64)
      {
        throw new Sitecore.Exceptions.ConfigurationException("The number of partitions is too high. Maximum of 64 partitions is supported.");
      }

      if (partitions <= 1)
      {
        this.m_Store = new SimpleMongoSessionStateStore(connectionString, compression);
      }
      else
      {
        this.m_Store = new PartitionedMongoSessionStateStore(connectionString, partitions, true);
      }

      bool firstTime = Interlocked.CompareExchange(ref initialized, 1, 0) == 0;

      if (firstTime)
      {
        Task.Run(() => this.m_Store.Initialize());
      }

      lock (ListSyncRoot)
      {
        MongoSessionStateProvidersList.Add(this);
      }
      this.CanStartTimer = this.IsTimerOffForAllInstance;
    }

    /// <summary>
    /// Releases managed and unmanaged resources.
    /// </summary>
    public override void Dispose()
    {
      lock (ListSyncRoot)
      {
        MongoSessionStateProvidersList.Remove(this);
        if (this.TimerEnabled && MongoSessionStateProvidersList.Count > 0)
        {
          foreach (var mongoSessionStateProvider in MongoSessionStateProvidersList)
          {
            if (mongoSessionStateProvider.m_ApplicationName == this.m_ApplicationName &&
                mongoSessionStateProvider.TriedToStartTimer)
            {
              mongoSessionStateProvider.StartTimer();
              break;
            }
          }
        }
      }
      base.Dispose();
    }

    /// <summary>
    ///   Adds a new session-state item to the data store.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The unique identifier of session the new state store item represents.
    /// </param>
    /// <param name="timeout">
    ///   The session timeout, in minutes, for the current request.
    /// </param>
    /// <remarks>
    ///   <para>
    ///     The <see cref="CreateUninitializedItem( HttpContext, string, int )"/> method is used with sessions when the
    ///     <i>cookieless</i> and <i>regenerateExpiredSessionId</i> attributes are both <c>true</c>. Having the
    ///     <i>regenerateExpiredSessionId</i> attribute set to <c>true</c> causes the <see cref="SessionStateModule"/>
    ///     object to generate a new session ID value when an expired session ID value is encountered.
    ///   </para>
    ///   <para>
    ///     The process of generating a new session ID value requires redirecting the browser to a URL that contains
    ///     the newly generated session ID value. The <see cref="CreateUninitializedItem( HttpContext, string, int )"/>
    ///     method is called during the initial request that contains an expired session ID value. After the
    ///     <see cref="SessionStateModule"/> object acquires a new session ID value to replace the expired value, it
    ///     calls the <see cref="CreateUninitializedItem( HttpContext, string, int )"/> method to add an uninitialized
    ///     entry to the session-state data store. The browser is then redirected to the URL containing the newly
    ///     generated session ID value. The existence of the uninitialized entry in the session data store ensures that
    ///     the redirected request that includes the newly generated session ID value is not mistaken for a request for
    ///     an expired session and is, instead, treated as a new session. 
    ///   </para>
    ///   <para>
    ///     The uninitialized entry in the session data store is associated with the newly generated session ID value
    ///     and contains only default values, including an expiration date and time and a value that corresponds to the
    ///     action flags parameter of the <see cref="GetItem"/> and <see cref="GetItemExclusive"/> methods. The
    ///     uninitialized entry in the session-state store should include an action flags value equal to the
    ///     <see cref="SessionStateActions.InitializeItem"/> enumeration value. This value is passed to the
    ///     <see cref="SessionStateModule"/> object by the <see cref="GetItem"/> and <see cref="GetItemExclusive"/>
    ///     methods, and informs the <see cref="SessionStateModule"/> object that the current session is a new but
    ///     uninitialized session. The <see cref="SessionStateModule"/> object will then initialize the new session and
    ///     raise the <c>Session_OnStart</c> event.
    ///   </para>
    /// </remarks>
    public override void CreateUninitializedItem([NotNull] HttpContext context, string id, int timeout)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");

      const int flags = ((int)SessionStateActions.InitializeItem);
      SessionStateItemCollection sessionItems = new SessionStateItemCollection();
      HttpStaticObjectsCollection staticObjects = new HttpStaticObjectsCollection();
      SessionStateStoreData sessionState = new SessionStateStoreData(sessionItems, staticObjects, timeout);

      this.m_Store.InsertItem(this.m_ApplicationName, id, flags, sessionState);
    }

    /// <summary>
    ///   Returns read-only session-state data from the session data store.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The session ID for the current request.
    /// </param>
    /// <param name="locked">
    ///   When this method returns, contains a <see cref="Boolean"/> value that is set to <c>true</c> if the requested
    ///   session item is locked at the session data store; otherwise, <c>false</c>.
    /// </param>
    /// <param name="lockAge">
    ///   When this method returns, contains a <see cref="TimeSpan"/> value that is set to the amount of time that the
    ///   item in the session data store has been locked or an <see cref="TimeSpan.Zero"/> if the item is not locked.
    /// </param>
    /// <param name="lockId">
    ///   When this method returns, contains an object that is lock identifier set on the item or <c>null</c> if the
    ///   item is not locked.
    /// </param>
    /// <param name="actions">
    ///   When this method returns, contains one of the <see cref="SessionStateActions"/> values, indicating whether
    ///   the current session is an uninitialized, cookieless session.
    /// </param>
    /// <returns>
    ///   A <see cref="SessionStateStoreData"/> object containing the requested session state data or <c>null</c> if
    ///   the item does not exist.
    /// </returns>
    public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");

      locked = false;
      lockAge = TimeSpan.Zero;
      lockId = null;
      actions = SessionStateActions.None;

      int flags = 0;
      Sitecore.SessionProvider.SessionStateLockCookie lockCookie = null;

      SessionStateStoreData result = this.Store.GetItem(this.m_ApplicationName, id, out lockCookie, out flags);

      if (null != result)
      {
        actions = ((SessionStateActions)flags);

        if (null != lockCookie)
        {
          locked = true;
          lockId = lockCookie.Id;
          lockAge = (DateTime.UtcNow - lockCookie.Timestamp);

          return null;
        }
      }

      return result;
    }

    /// <summary>
    ///   Locks and returns session state data from the session data store.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The session ID for the current request.
    /// </param>
    /// <param name="locked">
    ///   When this method returns, contains a <see cref="bool"/> value that is set to <c>true</c> if a lock is
    ///   successfully obtained; otherwise, <c>false</c>.
    /// </param>
    /// <param name="lockAge">
    ///   When this method returns, contains a <see cref="TimeSpan"/> value that is set to the amount of time that the
    ///   item in the session data store has been locked or an <see cref="TimeSpan.Zero"/> if the lock was obtained in
    ///   the current call.
    /// </param>
    /// <param name="lockId">
    ///   When this method returns, contains an object that is set to the lock identifier for the current request.
    /// </param>
    /// <param name="actions">
    ///   When this method returns, contains one of the <see cref="SessionStateActions"/> values, indicating whether
    ///   the current session is an uninitialized, cookieless session.
    /// </param>
    /// <returns>
    ///   A <see cref="SessionStateStoreData"/> object containing the session state data if the requested session state
    ///   store item was succefull locked; otherwise, <c>null</c>.
    /// </returns>
    /// <remarks>
    ///   More information can be found <see cref="SessionStateStoreProviderBase.GetItemExclusive">here</see> and
    ///   <see href="http://msdn.microsoft.com/en-us/library/dd941992.aspx">here</see>.
    /// </remarks>
    public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");

      //
      // Set the initial values of the out parameters. These are the values to be returned if the requested session
      // state store item does not exist.
      //

      locked = false;
      lockAge = TimeSpan.Zero;
      lockId = null;
      actions = SessionStateActions.None;

      SessionStateStoreData result = null;

      //
      // Run in a loop until we either successfully acquire the lock or we determin that we cannot lock the item as it
      // is already locked.
      //

      bool retry = true;

      while (true == retry)
      {
        int flags = 0;
        Sitecore.SessionProvider.SessionStateLockCookie lockCookie = Sitecore.SessionProvider.SessionStateLockCookie.Generate(DateTime.UtcNow);

        result = this.Store.GetItemExclusive(this.m_ApplicationName, id, lockCookie, out flags);

        if (null != result)
        {
          //
          // The lock was successfully acquired.
          //

          locked = false;
          lockId = lockCookie.Id;
          lockAge = TimeSpan.Zero;
          actions = ((SessionStateActions)flags);

          retry = false;
        }
        else
        {
          //
          // The lock was not acquired either because the session state entry is already locked or because the session
          // state entry doesn't exist. Try to read the current lock to determine which is the case.
          //

          lockCookie = this.Store.GetItemLock(this.m_ApplicationName, id);

          if (null != lockCookie)
          {
            //
            // A lock object was returned which means that the session state store entry exists.
            //

            if (true == lockCookie.IsLocked)
            {
              locked = true;
              lockId = lockCookie.Id;
              lockAge = (DateTime.UtcNow - lockCookie.Timestamp);
              actions = SessionStateActions.None;

              retry = false;
            }
            else
            {
              //
              // The session state entry exists, but it is no longer locked. We can try to acquaire the lock again.
              //

              retry = true;
            }
          }
          else
          {
            //
            // The session state store item was not found.
            //

            locked = false;
            lockId = null;
            lockAge = TimeSpan.Zero;
            actions = SessionStateActions.None;

            retry = false;
          }
        }
      }

      return result;
    }

    /// <summary>
    ///   Updates the session-item information in the session-state data store with values from the current request,
    ///   and clears the lock on the data.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The session ID for the current request.
    /// </param>
    /// <param name="sessionState">
    ///   The <see cref="SessionStateStoreData"/> object that contains the current session values to be stored.
    /// </param>
    /// <param name="lockId">
    ///   The lock identifier for the current request.
    /// </param>
    /// <param name="newItem">
    ///   <c>true</c> to identify the session item as a new item; otherwise, <c>false</c>.
    /// </param>
    /// <remarks>
    ///   <para>
    ///     The <see cref="SessionStateModule"/> object calls the SetAndReleaseItemExclusive method at the end of a
    ///     request, during the <see cref="HttpApplication.ReleaseRequestState"/> event, to insert current session-
    ///     item information into the data store or update existing session-item information in the data store with
    ///     current values, to update the expiration time on the item, and to release the lock on the data. Only
    ///     session data for the current application that matches the supplied session id and lock id values is
    ///     updated.
    ///   </para>
    ///   <para>
    ///     If the session values for the current request have not been modified, the
    ///     <see cref="SetAndReleaseItemExclusive"/> method is not called. Instead, the
    ///     <see cref="ReleaseItemExclusive"/> method is called.
    ///   </para>
    ///   <para>
    ///     If the <see cref="HttpSessionState.Abandon"/> method has been called, the
    ///     <see cref="SetAndReleaseItemExclusive"/> method is not called. Instead, the
    ///     <see cref="SessionStateModule"/> object calls the <see cref="RemoveItem"/> method to delete session-item
    ///     data from the data source.
    ///   </para>
    /// </remarks>
    public override void SetAndReleaseItemExclusive([NotNull] HttpContext context, string id, SessionStateStoreData sessionState, object lockId, bool newItem)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");

      if (newItem)
      {
        const int flags = ((int)SessionStateActions.None);
        this.Store.InsertItem(this.m_ApplicationName, id, flags, sessionState);
      }
      else
      {
        string lockCookie = System.Convert.ToString(lockId);
        this.Store.UpdateAndReleaseItem(this.m_ApplicationName, id, lockCookie, SessionStateActions.None, sessionState);
      }
    }

    /// <summary>
    ///   Releases a lock on an item in the session data store.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The session ID for the current request.
    /// </param>
    /// <param name="lockId">
    ///   The lock identifier for the current request.
    /// </param>
    /// <remarks>
    ///   <para>
    ///     The <see cref="SessionStateModule"/> object calls the <see cref="ReleaseItemExclusive"/> method to update
    ///     the expiration date and release a lock on an item in the session data store. It is called at the end of a
    ///     request, during the <see cref="HttpApplication.ReleaseRequestState"/> event, if session values are
    ///     unchanged. If session values have been modified, the <see cref="SessionStateModule"/> object instead calls
    ///     the <see cref="SetAndReleaseItemExclusive"/> method.
    ///   </para>
    ///   <para>
    ///     The <see cref="SessionStateModule"/> object also calls the <see cref="ReleaseItemExclusive"/> method when a
    ///     lock on an item in the session data store has exceeded the <see cref="HttpRuntimeSection.ExecutionTimeout"/>
    ///     value. For more information about locking and details about the lock identifier, see "Locking Session-Store
    ///     Data" in the <see cref="SessionStateStoreProviderBase"/> class overview.
    ///   </para>
    ///   <para>
    ///     The <see cref="ReleaseItemExclusive"/> method only removes the lock from an item in the session data store
    ///     for the current application that matches the supplied session id and lock id values. If the lock id does not
    ///     match the one in the data store, the <see cref="ReleaseItemExclusive"/> method does nothing.
    ///   </para>
    /// </remarks>
    public override void ReleaseItemExclusive([NotNull] HttpContext context, string id, object lockId)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(lockId, "lockId");

      string lockCookie = System.Convert.ToString(lockId);

      this.Store.ReleaseItem(this.m_ApplicationName, id, lockCookie);
    }

    /// <summary>
    ///   Updates the expiration date and time of an item in the session data store.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The session ID for the current request.
    /// </param>
    /// <remarks>
    ///   The <see cref="SessionStateModule"/> object calls the <see cref="ResetItemTimeout"/> method to update the
    ///   expiration date and time for a session to the current date and time plus the session
    ///   <see cref="HttpSessionState.Timeout"/> value.
    /// </remarks>
    public override void ResetItemTimeout([NotNull] HttpContext context, string id)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");

      this.Store.UpdateItemExpiration(this.m_ApplicationName, id);
    }

    /// <summary>
    ///   Deletes item data from the session data store.
    /// </summary>
    /// <param name="context">
    ///   The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="id">
    ///   The session ID for the current request.
    /// </param>
    /// <param name="lockId">
    ///   The lock identifier for the current request.
    /// </param>
    /// <param name="item">
    ///   The <see cref="SessionStateStoreData"/> that represents the item to delete from the data store.
    /// </param>
    /// <remarks>
    ///   <para>
    ///     The <see cref="SessionStateModule"/> object calls the <see cref="RemoveItem"/> method at the end of a
    ///     request, during the <see cref="ReleaseRequestState"/> event, to delete the data for a session item from
    ///     the session data store if the <see cref="HttpSessionState.Abandon"/> method has been called. Only session
    ///     data for the current application that matches the supplied session id and lock id values is deleted. For
    ///     more information about locking and details about the lock identifier, see "Locking Session-Store Data" in
    ///     the <see cref="SessionStateStoreProviderBase"/> class overview.
    ///   </para>
    /// </remarks>
    public override void RemoveItem([NotNull] HttpContext context, string id, object lockId, [CanBeNull] SessionStateStoreData item)
    {
      Sitecore.Diagnostics.Assert.ArgumentNotNull(context, "context");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(id, "id");
      Sitecore.Diagnostics.Assert.ArgumentNotNull(lockId, "lockId");

      string lockCookie = System.Convert.ToString(lockId);

      try
      {
        this.ExecuteSessionEnd(id, item);
      }
      finally
      {
        this.Store.RemoveItem(this.m_ApplicationName, id, lockCookie);
      }
    }

    /// <summary>
    /// The get expired item exclusive.
    /// </summary>
    /// <param name="signalTime">
    /// The signal time.
    /// </param>
    /// <param name="lockCookie">
    /// The lock cookie.
    /// </param>
    /// <param name="id">
    /// The id.
    /// </param>
    /// <returns>
    /// Returns Session State object with exclusive lock<see cref="SessionStateStoreData"/>.
    /// </returns>
    protected override SessionStateStoreData GetExpiredItemExclusive(DateTime signalTime, Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out string id)
    {
      return this.Store.GetExpiredItemExclusive(this.m_ApplicationName, signalTime, lockCookie, out id);
    }

    /// <summary>
    /// The remove item.
    /// </summary>
    /// <param name="id">
    /// The id.
    /// </param>
    /// <param name="lockCookie">
    /// The lock cookie identifier.
    /// </param>
    protected override void RemoveItem(string id, string lockCookie)
    {
      this.Store.RemoveItem(this.m_ApplicationName, id, lockCookie);
    }

    #region Private methods

    /// <summary>
    /// Check whether the timer is off for all instance
    /// </summary>
    /// <returns>True if timer is off otherwise False</returns>
    private bool IsTimerOffForAllInstance()
    {
      lock (ListSyncRoot)
      {
        foreach (var mongoSessionStateProvider in MongoSessionStateProvidersList)
        {
          if (mongoSessionStateProvider.m_ApplicationName == this.m_ApplicationName &&
              mongoSessionStateProvider.TriedToStartTimer &&
              mongoSessionStateProvider.TimerEnabled)
            return false;
        }
        return true;
      }
    }

    #endregion
  }
}