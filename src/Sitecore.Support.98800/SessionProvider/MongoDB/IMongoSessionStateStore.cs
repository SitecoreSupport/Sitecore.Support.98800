using System;
using System.Web.SessionState;
using MongoDB.Driver;

namespace Sitecore.Support.SessionProvider.MongoDB
{
  internal interface IMongoSessionStateStore
  {
    void Initialize();

    [CanBeNull]
    SessionStateStoreData GetItem([NotNull] string application, [NotNull] string id, out Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags);

    /// <summary>
    ///   Attempts to lock and load the session state store item with the specified identifier.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The unique identifier of the session state store entry to lock.
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
    [CanBeNull]
    SessionStateStoreData GetItemExclusive([NotNull] string application, [NotNull] string id, [NotNull] Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags);

    /// <summary>
    ///   Attempts to lock and load an expired session state store item.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="signalTime">
    ///   The date and time the signal for processing expired sessions was generated, expressed as UTC.
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
    /// <remarks><c>DateTime.Kind</c> should be <c>DateTimeKind.Utc</c>.</remarks>
    [CanBeNull]
    SessionStateStoreData GetExpiredItemExclusive([NotNull] string application, DateTime signalTime, [NotNull] Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out string id);

    /// <summary>
    ///   Gets the <see cref="SessionStateLockCookie"/> from the session state entry with the specified unique
    ///   identifier.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The unique identifier of the session state store entry.
    /// </param>
    /// <returns>
    ///   The <see cref="SessionStateLockCookie"/> object containing the lock state of the session state store entry with
    ///   the specified unique identifier; or <c>null</c> if the session state store entry does not exist.
    /// </returns>
    [CanBeNull]
    Sitecore.SessionProvider.SessionStateLockCookie GetItemLock([NotNull] string application, [NotNull] string id);

    /// <summary>
    ///   Updates a locked session state store item and releases the lock.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The session state store item to update.
    /// </param>
    /// <param name="lockCookie">
    ///   The identifier of the lock currently held.
    /// </param>
    /// <param name="action">
    ///   A <see cref="SessionStateActions"/> value
    /// </param>
    /// <param name="sessionState">
    ///   A <see cref="SessionStateStoreData"/> that represents the session data.
    /// </param>
    void UpdateAndReleaseItem([NotNull] string application, [NotNull] string id, [NotNull] string lockCookie, SessionStateActions action, [NotNull] SessionStateStoreData sessionState);

    /// <summary>
    ///   Releases a locked session state store item.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The session state store item to release.
    /// </param>
    /// <param name="lockCookie">
    ///   The identifier of the lock currently held.
    /// </param>
    void ReleaseItem([NotNull] string application, [NotNull] string id, [NotNull] string lockCookie);

    /// <summary>
    ///   Removes a locked session state store item from the database.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The session state store item to release.
    /// </param>
    /// <param name="lockCookie">
    ///   The identifier of the lock currently held.
    /// </param>
    void RemoveItem([NotNull] string application, [NotNull] string id, [NotNull] string lockCookie);

    /// <summary>
    ///   Inserts a new session state store item into the sessions collection in the Mongo database.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The unique identifier of the session state store item.
    /// </param>
    /// <param name="flags">
    ///   An <see cref="Int32"/> value that is a set of flags stored together with the session state store item.
    /// </param>
    /// <param name="sessionState">
    ///   A <see cref="SessionStateStoreData"/> object that represents the session state.
    /// </param>
    /// <remarks>
    ///   This method insert a new document into the sessions collection. The expiration field is set to the current
    ///   timestamp incremented by the timeout value specified in the session state. The new session state store 
    ///   item is not locked.
    /// </remarks>
    /// <exception cref="MongoException">
    ///   A session state store item with the specified identifier already exists.
    /// </exception>
    void InsertItem([NotNull] string application, [NotNull] string id, int flags, [NotNull] SessionStateStoreData sessionState);

    /// <summary>
    ///   Updates the expiration time of session state store item.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="id">
    ///   The unique identifier of the session state store item to update.
    /// </param>
    /// <remarks>
    ///   This method loads the timeout value of the specified session state store item and updates the expiration time
    ///   field to the current time incremented by the retrieved timeout value.
    /// </remarks>
    void UpdateItemExpiration([NotNull] string application, [NotNull] string id);
  }
}