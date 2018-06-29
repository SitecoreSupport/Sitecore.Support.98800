using System;
using System.Collections.Specialized;
using System.Timers;
using System.Web;
using System.Web.SessionState;
using Sitecore.Diagnostics;
using Sitecore.Exceptions;
using Sitecore.SessionProvider.Helpers;

namespace Sitecore.Support.SessionProvider
{
  /// <summary>
  /// Sitecore session state provider base class.
  /// </summary>
  public abstract class SitecoreSessionStateStoreProvider : SessionStateStoreProviderBase
  {
    #region Fields

    /// <summary>
    /// The sync root object.
    /// </summary>
    [Obsolete]
    public static readonly object SyncRoot = new object();

    /// <summary>
    /// Delegate if concrete provider needs to control the timer
    /// </summary>
    protected Func<bool> CanStartTimer;

    /// <summary>
    /// Flag to identify if the instance tried to start the timer
    /// </summary>
    public bool TriedToStartTimer { get; private set; }

    /// <summary>
    /// Get the timer status and start if needed
    /// </summary>
    public bool TimerEnabled
    {
      get
      {
        return this.timer != null && this.timer.Enabled;
      }
    }
    /// <summary>
    /// The is processing flag.
    /// </summary>
    private bool isProcessing;

    /// <summary>
    /// The timer.
    /// </summary>
    private volatile Timer timer;

    /// <summary>
    /// The polling interval.
    /// </summary>
    private int pollingInterval = 2;

    /// <summary>
    /// The session end callback.
    /// </summary>
    private SessionStateItemExpireCallback sessionEndCallback;

    private readonly object syncRoot = new object();

    #endregion

    #region Constructors

    /// <summary>
    /// Initializes a new instance of the <see cref="SitecoreSessionStateStoreProvider"/> class. 
    /// </summary>
    /// <param name="pollingInterval">
    /// The polling interval.
    /// </param>
    protected SitecoreSessionStateStoreProvider(int pollingInterval) :
      this()
    {
      this.SetPollingInterval(pollingInterval);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SitecoreSessionStateStoreProvider"/> class. 
    /// </summary>
    protected SitecoreSessionStateStoreProvider()
    {
      this.timer = new Timer();
      this.timer.AutoReset = true;
      this.timer.Elapsed += this.OnProcessExpiredItems;
      this.TriedToStartTimer = false;
    }

    #endregion

    #region Public methods

    /// <summary>
    /// Start the timer
    /// </summary>
    public void StartTimer()
    {
      if (this.timer != null)
        this.timer.Start();
    }

    /// <summary>
    /// Releases managed and unmanaged resources.
    /// </summary>
    public override void Dispose()
    {
      if (null != this.timer)
      {
        this.timer.Stop();
        this.timer.Dispose();
        this.timer = null;
      }
    }

    /// <summary>
    /// Initializes the current provider.
    /// </summary>
    /// <param name="name">
    /// The friendly name of the provider.
    /// </param>
    /// <param name="config">
    /// A collection of the name/value pairs representing the provider-specific attributes specified in the
    ///   configuration for this provider.
    /// </param>
    /// <exception cref="Sitecore.Exceptions.ConfigurationException">
    /// The polling interval specified is too small or too high.
    /// </exception>
    public override void Initialize(string name, NameValueCollection config)
    {
      base.Initialize(name, config);

      var configuration = new ConfigReader(config, name);

      this.SetPollingInterval(configuration.GetInt32("pollingInterval", this.pollingInterval));
    }

    /// <summary>
    /// Sets a reference to the <see cref="SessionStateItemExpireCallback"/> delegate for the <c>Session_OnEnd</c>
    ///   event defined in the <c>Global.asax</c> file.
    /// </summary>
    /// <param name="expireCallback">
    /// The <see cref="SessionStateItemExpireCallback"/> delegate for the <c>Session_OnEnd</c> event defined in the
    ///   <c>Global.asax</c> file.
    /// </param>
    /// <returns>
    /// <c>true</c> if the session-state store provider supports calling the <c>Session_OnEnd</c> event; otherwise,
    ///   <c>false</c>.
    /// </returns>
    public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
    {
      this.sessionEndCallback = expireCallback;

      if (expireCallback != null)
      {
        this.TriedToStartTimer = true;
        if (this.CanStartTimer == null || this.CanStartTimer())
        {
          this.StartTimer();
        }
        return true;
      }

      this.timer.Stop();
      return false;
    }

    /// <summary>
    /// Creates a new <see cref="SessionStateStoreData"/> object to be used for the current request.
    /// </summary>
    /// <param name="context">
    /// The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <param name="timeout">
    /// The session-state timeout value for the new <see cref="SessionStateStoreData"/>.
    /// </param>
    /// <returns>
    /// A new <see cref="SessionStateStoreData"/> for the current request.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Argument <paramref name="context"/> is a <c>null</c> reference.
    /// </exception>
    public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
    {
      if (context == null)
      {
        throw new ArgumentNullException("context");
      }

      HttpStaticObjectsCollection staticObjects = SessionStateUtility.GetSessionStaticObjects(context);
      var sessionItems = new SessionStateItemCollection();

      var result = new SessionStateStoreData(sessionItems, staticObjects, timeout);

      return result;
    }

    /// <summary>
    /// Called by the <see cref="SessionStateModule"/> object at the end of a request.
    /// </summary>
    /// <param name="context">
    /// The <see cref="SessionStateModule"/> object calls the <see cref="EndRequest"/> method at the end of a request
    ///   for an ASP.NET page, during the <see cref="EndRequest"/> event. The <see cref="EndRequest"/> method can be
    ///   used to perform any per-request cleanup required by your session-state store provider.
    /// </param>
    public override void EndRequest(HttpContext context)
    {
    }

    /// <summary>
    /// Called by the <see cref="SessionStateModule"/> object for per-request initialization.
    /// </summary>
    /// <param name="context">
    /// The <see cref="HttpContext"/> for the current request.
    /// </param>
    /// <remarks>
    /// The <see cref="SessionStateModule"/> object calls the <see cref="InitializeRequest"/> method before calling
    ///   any other <see cref="SitecoreSessionStateStoreProvider"/> method. The <see cref="InitializeRequest"/> method can be
    ///   used to perform any per-request initialization required by the session-state store provider.
    /// </remarks>
    public override void InitializeRequest(HttpContext context)
    {
    }

    #endregion

    #region Protected methods

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
    protected abstract SessionStateStoreData GetExpiredItemExclusive(DateTime signalTime, Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out string id);

    /// <summary>
    /// The on process expired items.
    /// </summary>
    /// <param name="signalTime">
    /// The signal time.
    /// </param>
    /// <returns>
    /// The unique identifier of item<see cref="string"/>.
    /// </returns>
    protected virtual string OnProcessExpiredItems(DateTime signalTime)
    {
      string id;

      Sitecore.SessionProvider.SessionStateLockCookie cookie = Sitecore.SessionProvider.SessionStateLockCookie.Generate(signalTime);
      SessionStateStoreData item = this.GetExpiredItemExclusive(signalTime, cookie, out id);

      if (item != null)
      {
        try
        {
          this.ExecuteSessionEnd(id, item);
        }
        finally
        {
          this.RemoveItem(id, cookie.Id);
        }
      }

      return id;
    }

    /// <summary>
    /// Executes the <c>Session_OnEnd</c> event defined in the <c>Global.asax</c> file.
    /// </summary>
    /// <param name="id">
    /// The unique identifier of the session.
    /// </param>
    /// <param name="item">
    /// The <see cref="SessionStateStoreData"/> object that contains the current session values.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Argument <paramref name="id"/> or <paramref name="item"/> is a <c>null</c> reference.
    /// </exception>
    protected void ExecuteSessionEnd(string id, SessionStateStoreData item)
    {
      if (id == null)
      {
        throw new ArgumentNullException("id");
      }

      if (item == null)
      {
        throw new ArgumentNullException("item");
      }

      if (null != this.sessionEndCallback)
      {
        try
        {
          this.sessionEndCallback.Invoke(id, item);
        }
        catch (Exception ex)
        {
          Log.Error("Error executing the session end callback. Id: " + id, ex, this);
        }
      }
    }

    /// <summary>
    /// Removes item from Session states storage.
    /// </summary>
    /// <param name="id">
    /// The identifier of session object.
    /// </param>
    /// <param name="lockCookie">
    /// The lock cookie.
    /// </param>
    protected abstract void RemoveItem(string id, string lockCookie);

    #endregion

    #region Private methods

    /// <summary>
    /// Processes expired items.
    /// </summary>
    /// <param name="sender">
    /// The object that generated the event.
    /// </param>
    /// <param name="args">
    /// A <see cref="ElapsedEventArgs"/> object that provides additional information about the event.
    /// </param>
    /// <remarks>
    /// The <see cref="OnProcessExpiredItems(object, ElapsedEventArgs)"/> queries the session state store for expired items. If an expired
    ///   item is found, the item is locked and a <c>Session_OnEnd</c>event is generated. The item is removed after the
    ///   event. This process is repeated until no more expired items are found.
    /// </remarks>
    private void OnProcessExpiredItems([NotNull] object sender, [NotNull] ElapsedEventArgs args)
    {
      Debug.ArgumentNotNull(sender, "sender");
      Debug.ArgumentNotNull(args, "args");

      lock (this.syncRoot)
      {
        if (this.isProcessing)
        {
          return;
        }

        this.isProcessing = true;
      }

      try
      {
        if (this.timer == null)
        {
          // the provider instance is being disposed, so we abort. Also below.
          return;
        }

        bool found;

        do
        {
          DateTime signalTime = args.SignalTime.ToUniversalTime();
          found = this.OnProcessExpiredItems(signalTime) != null;
        }
        while ((this.timer != null) && found);
      }
      catch (Exception)
      {
        Log.SingleError("Failed processing expired items. These will be retried according to the pollingInterval.", this);
        throw;
      }
      finally
      {
        lock (this.syncRoot)
        {
          this.isProcessing = false;
        }
      }
    }

    /// <summary>
    /// Set polling interval.
    /// </summary>
    /// <param name="interval">
    /// The interval.
    /// </param>
    private void SetPollingInterval(int interval)
    {
      if (interval < 1)
      {
        throw new ConfigurationException("The polling interval specified is too small.");
      }

      if (interval > 120)
      {
        throw new ConfigurationException("The polling interval specified is too high.");
      }

      this.pollingInterval = interval;

      this.timer.Interval = 1000d * this.pollingInterval;
    }

    #endregion
  }
}