using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web.Configuration;
using System.Web.SessionState;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Sitecore.Diagnostics;

namespace Sitecore.Support.SessionProvider.MongoDB
{
  internal class MongoSessionStateStore
  {
    private const string COLLECTION_NAME = "sessions";

    private const string FIELD_ID = "_id";
    private const string FIELD_SESSION_ID = "s";
    private const string FIELD_APPLICATION = "a";
    private const string FIELD_LOCK_COOKIE = "lc";
    private const string FIELD_LOCK_TIMESTAMP = "lt";
    private const string FIELD_FLAGS = "f";
    private const string FIELD_TIMEOUT = "t";
    private const string FIELD_DATA = "d";
    private const string FIELD_EXPIRATION = "e";

    private static readonly int DefaultSessionTimeout;

    private readonly bool m_Compress;
    private readonly MongoCollection m_Reader;
    private readonly MongoCollection m_Writer;



    /// <summary>
    /// Initializes static members of the <see cref="MongoSessionStateStore"/> class. 
    /// </summary>
    static MongoSessionStateStore()
    {
      var sessionStateSection = ConfigurationManager.GetSection("system.web/sessionState") as SessionStateSection;
      if (sessionStateSection != null)
      {
        DefaultSessionTimeout = (int)sessionStateSection.Timeout.TotalMinutes;
      }
      else
      {
        DefaultSessionTimeout = 20; // some hardcoded value.
      }
    }



    internal MongoSessionStateStore([NotNull] string connectionString, bool compress)
    {
      Debug.ArgumentNotNull(connectionString, "connectionString");

      this.m_Reader = GetCollection(connectionString, COLLECTION_NAME);
      this.m_Writer = GetCollection(connectionString, COLLECTION_NAME);

      this.m_Compress = compress;
    }



    internal MongoSessionStateStore([NotNull] string connectionString, [NotNull] string databaseName, bool compress)
    {
      Debug.ArgumentNotNull(connectionString, "connectionString");
      Debug.ArgumentNotNull(databaseName, "databaseName");

      this.m_Reader = GetCollection(connectionString, databaseName, COLLECTION_NAME);
      this.m_Writer = GetCollection(connectionString, databaseName, COLLECTION_NAME);

      this.m_Compress = compress;
    }



    [NotNull]
    protected BsonDocument BuildSessionIdentifier([NotNull] string application, [NotNull] string id)
    {
      Debug.ArgumentNotNull(application, "application");
      Debug.ArgumentNotNull(id, "id");

      BsonString bsonApplicationId = new BsonString(application);
      BsonString bsonSessionId = new BsonString(id);

      BsonDocument result = new BsonDocument();

      result.Add(FIELD_SESSION_ID, bsonSessionId);
      result.Add(FIELD_APPLICATION, bsonApplicationId);

      return result;
    }



    [NotNull]
    protected IMongoQuery BuildQueryBySessionIdentifier([NotNull] string application, [NotNull] string id)
    {
      Debug.ArgumentNotNull(application, "application");
      Debug.ArgumentNotNull(id, "id");

      BsonDocument bsonId = this.BuildSessionIdentifier(application, id);
      IMongoQuery result = Query.EQ(FIELD_ID, bsonId);

      return result;
    }



    [NotNull]
    private static MongoCollection GetCollection([NotNull] string connectionString, [NotNull] string collectionName)
    {
      Debug.ArgumentNotNull(connectionString, "connectionString");
      Debug.ArgumentNotNull(collectionName, "collectionName");

      MongoUrl url = new MongoUrl(connectionString);
      MongoClient client = new MongoClient(url);
      MongoServer server = client.GetServer();
      MongoDatabase database = server.GetDatabase(url.DatabaseName);
      MongoCollection result = database.GetCollection(collectionName);

      return result;
    }



    [NotNull]
    private static MongoCollection GetCollection([NotNull] string connectionString, [NotNull] string databaseName, [NotNull] string collectionName)
    {
      Debug.ArgumentNotNull(connectionString, "connectionString");
      Debug.ArgumentNotNull(databaseName, "databaseName");
      Debug.ArgumentNotNull(collectionName, "collectionName");

      MongoUrl url = new MongoUrl(connectionString);
      MongoClient client = new MongoClient(url);
      MongoServer server = client.GetServer();
      MongoDatabase database = server.GetDatabase(databaseName);
      MongoCollection result = database.GetCollection(collectionName);

      return result;
    }



    internal void Initialize()
    {
      this.m_Writer.EnsureIndex(FIELD_EXPIRATION, FIELD_ID + "." + FIELD_SESSION_ID, FIELD_ID + "." + FIELD_APPLICATION);
    }



    internal SessionStateStoreData GetItem(string application, string id, out Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      flags = 0;
      lockCookie = null;

      SessionStateStoreData result = null;

      BsonDateTime bsonExpiration = new BsonDateTime(DateTime.UtcNow.AddMinutes(DefaultSessionTimeout));

      IMongoQuery query = this.BuildQueryBySessionIdentifier(application, id);

      var updateExpiration = new UpdateBuilder();
      updateExpiration.Set(FIELD_EXPIRATION, bsonExpiration);

      IMongoFields fields = Fields.Include(FIELD_LOCK_COOKIE, FIELD_LOCK_TIMESTAMP, FIELD_FLAGS, FIELD_DATA);

      FindAndModifyResult famr = this.m_Writer.FindAndModify(query, null, updateExpiration, fields, true, false);

      if (famr.Ok && (null != famr.ModifiedDocument))
      {
        flags = famr.ModifiedDocument[FIELD_FLAGS].ToInt32();
        string lockId = famr.ModifiedDocument[FIELD_LOCK_COOKIE].ToString();
        DateTime lockTs = famr.ModifiedDocument[FIELD_LOCK_TIMESTAMP].ToUniversalTime();

        if (!string.IsNullOrWhiteSpace(lockId))
        {
          lockCookie = new Sitecore.SessionProvider.SessionStateLockCookie(lockId, lockTs);
        }

        result = Sitecore.SessionProvider.SessionStateSerializer.Deserialize(famr.ModifiedDocument[FIELD_DATA].AsByteArray);

        if (result.Timeout != DefaultSessionTimeout)
        {
          this.UpdateItemExpiration(application, id, result.Timeout);
        }
      }

      return result;
    }


    /// <summary>
    /// Locks the item in the store and extends its time.
    /// </summary>
    /// <param name="application">
    /// The application.
    /// </param>
    /// <param name="id">
    /// The id.
    /// </param>
    /// <param name="lockCookie">
    /// The lock cookie.
    /// </param>
    /// <param name="flags">
    /// The flags.
    /// </param>
    /// <returns>
    /// The <see cref="SessionStateStoreData"/>, or null if it couldn't be locked.
    /// </returns>
    internal SessionStateStoreData GetItemExclusive(string application, string id, Sitecore.SessionProvider.SessionStateLockCookie lockCookie, out int flags)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      flags = 0;

      SessionStateStoreData result = null;

      BsonString bsonLockCookie = new BsonString(lockCookie.Id);
      BsonDateTime bsonLockTimestamp = new BsonDateTime(lockCookie.Timestamp);
      BsonDateTime bsonDefaultExpiration = new BsonDateTime(DateTime.UtcNow.AddMinutes(DefaultSessionTimeout));

      IMongoQuery querySessionIdMatches = this.BuildQueryBySessionIdentifier(application, id);
      IMongoQuery queryLockCookieIsNotSet = Query.EQ(FIELD_LOCK_COOKIE, BsonString.Empty);
      IMongoQuery query = Query.And(querySessionIdMatches, queryLockCookieIsNotSet);

      UpdateBuilder update = new UpdateBuilder();

      update.Set(FIELD_EXPIRATION, bsonDefaultExpiration);
      update.Set(FIELD_LOCK_COOKIE, bsonLockCookie);
      update.Set(FIELD_LOCK_TIMESTAMP, bsonLockTimestamp);

      FindAndModifyResult famr = this.m_Writer.FindAndModify(query, null, update, true, false);

      if (famr.Ok && (null != famr.ModifiedDocument))
      {
        flags = famr.ModifiedDocument[FIELD_FLAGS].AsInt32;
        result = Sitecore.SessionProvider.SessionStateSerializer.Deserialize(famr.ModifiedDocument[FIELD_DATA].AsByteArray);

        if (result.Timeout != DefaultSessionTimeout)
        {
          this.UpdateItemExpiration(application, id, result.Timeout);
        }
      }

      return result;
    }



    internal Sitecore.SessionProvider.SessionStateLockCookie GetItemLock(string application, string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      Sitecore.SessionProvider.SessionStateLockCookie result = null;

      IMongoQuery query = this.BuildQueryBySessionIdentifier(application, id);

      MongoCursor<BsonDocument> cursor = this.m_Reader.FindAs<BsonDocument>(query);

      cursor.SetFields(FIELD_LOCK_COOKIE, FIELD_LOCK_TIMESTAMP, FIELD_FLAGS);

      BsonDocument document = cursor.FirstOrDefault();

      if (null != document)
      {
        string lockCookie = document[FIELD_LOCK_COOKIE].ToString();
        DateTime lockTimestamp = document[FIELD_LOCK_TIMESTAMP].ToUniversalTime();

        result = new Sitecore.SessionProvider.SessionStateLockCookie(lockCookie, lockTimestamp);
      }

      return result;
    }



    internal void UpdateAndReleaseItem(string application, string id, string lockCookie, SessionStateActions action, SessionStateStoreData sessionState)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");
      Assert.ArgumentNotNull(sessionState, "sessionState");

      byte[] data = Sitecore.SessionProvider.SessionStateSerializer.Serialize(sessionState, this.m_Compress);
      DateTime expiration = DateTime.UtcNow.AddMinutes(sessionState.Timeout);

      BsonString bsonLockId = new BsonString(lockCookie);

      IMongoQuery querySessionIdMatches = this.BuildQueryBySessionIdentifier(application, id);
      IMongoQuery queryLockIsSet = Query.EQ(FIELD_LOCK_COOKIE, bsonLockId);
      IMongoQuery query = Query.And(querySessionIdMatches, queryLockIsSet);

      BsonDateTime bsonExpiration = new BsonDateTime(expiration);
      BsonInt32 bsonAction = new BsonInt32((int)action);
      BsonInt32 bsonTimeout = new BsonInt32(sessionState.Timeout);
      BsonBinaryData bsonData = new BsonBinaryData(data);

      UpdateBuilder update = new UpdateBuilder();

      update.Set(FIELD_EXPIRATION, bsonExpiration);
      update.Set(FIELD_FLAGS, bsonAction);
      update.Set(FIELD_LOCK_COOKIE, BsonString.Empty);
      update.Set(FIELD_TIMEOUT, bsonTimeout);
      update.Set(FIELD_DATA, bsonData);

      WriteConcernResult wcr = this.m_Writer.Update(query, update, UpdateFlags.None, WriteConcern.Acknowledged);

      Debug.Assert(wcr != null && !wcr.HasLastErrorMessage, "Failed to release the session state store item.");
    }



    internal void ReleaseItem(string application, string id, string lockCookie)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      BsonString bsonLockCookie = new BsonString(lockCookie);

      IMongoQuery querySessionIdMatches = this.BuildQueryBySessionIdentifier(application, id);
      IMongoQuery queryLockCookieIsSet = Query.EQ(FIELD_LOCK_COOKIE, bsonLockCookie);
      IMongoQuery query = Query.And(querySessionIdMatches, queryLockCookieIsSet);

      UpdateBuilder update = new UpdateBuilder();

      update.Set(FIELD_LOCK_COOKIE, BsonString.Empty);

      WriteConcernResult wcr = this.m_Writer.Update(query, update, UpdateFlags.None, WriteConcern.Acknowledged);

      Debug.Assert(wcr != null && !wcr.HasLastErrorMessage, "Failed to release the session state store item.");
    }



    internal void RemoveItem(string application, string id, string lockCookie)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      BsonString bsonLockCookie = new BsonString(lockCookie);

      IMongoQuery querySessionIdMatches = this.BuildQueryBySessionIdentifier(application, id);
      IMongoQuery queryLockCookieIsSet = Query.EQ(FIELD_LOCK_COOKIE, bsonLockCookie);
      IMongoQuery query = Query.And(querySessionIdMatches, queryLockCookieIsSet);

      WriteConcernResult wcr = this.m_Writer.Remove(query, RemoveFlags.Single, WriteConcern.Acknowledged);

      Debug.Assert(wcr != null && !wcr.HasLastErrorMessage, "Failed to remove the session state store item.");
    }



    internal void InsertItem(string application, string id, int flags, SessionStateStoreData sessionState)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");
      Assert.ArgumentNotNull(sessionState, "sessionState");

      DateTime timestamp = DateTime.UtcNow;
      DateTime expiration = timestamp.AddMinutes(sessionState.Timeout);

      byte[] data = Sitecore.SessionProvider.SessionStateSerializer.Serialize(sessionState, this.m_Compress);

      BsonDocument bsonId = this.BuildSessionIdentifier(application, id);
      BsonInt32 bsonTimeout = new BsonInt32(sessionState.Timeout);
      BsonDateTime bsonExpiration = new BsonDateTime(expiration);
      BsonString bsonLockId = new BsonString(string.Empty);
      BsonDateTime bsonLockTimestamp = new BsonDateTime(timestamp);
      BsonInt32 bsonAction = new BsonInt32(flags);
      BsonBinaryData bsonData = new BsonBinaryData(data);

      BsonDocument document = new BsonDocument(false);

      document.Add(FIELD_ID, bsonId);
      document.Add(FIELD_TIMEOUT, bsonTimeout);
      document.Add(FIELD_EXPIRATION, bsonExpiration);
      document.Add(FIELD_LOCK_COOKIE, bsonLockId);
      document.Add(FIELD_LOCK_TIMESTAMP, bsonLockTimestamp);
      document.Add(FIELD_FLAGS, bsonAction);
      document.Add(FIELD_DATA, bsonData);

      IMongoQuery query = Query.EQ(FIELD_ID, bsonId);

      WriteConcernResult wcr = this.m_Writer.Update(query, Update.Replace(document), UpdateFlags.Upsert, WriteConcern.Acknowledged);

      Debug.Assert(wcr != null && !wcr.HasLastErrorMessage, "Failed to insert the session state store item.");
    }



    internal void UpdateItemExpiration(string application, string id)
    {
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(id, "id");

      IMongoQuery query = this.BuildQueryBySessionIdentifier(application, id);

      MongoCursor<BsonDocument> cursor = this.m_Reader.FindAs<BsonDocument>(query);

      cursor.SetFields(FIELD_TIMEOUT);

      BsonDocument document = cursor.FirstOrDefault();

      if (null != document)
      {
        int timeout = document[FIELD_TIMEOUT].ToInt32();

        this.UpdateItemExpiration(application, id, timeout);
      }
    }



    internal void UpdateItemExpiration([NotNull] string application, [NotNull] string id, int timeout)
    {
      Debug.ArgumentNotNull(application, "application");
      Debug.ArgumentNotNull(id, "id");

      Debug.Assert(0 < timeout, "The specified item timeout is not valid.");

      IMongoQuery query = this.BuildQueryBySessionIdentifier(application, id);

      DateTime expiration = DateTime.UtcNow.AddMinutes(timeout);
      BsonDateTime bsonExpiration = new BsonDateTime(expiration);

      UpdateBuilder update = new UpdateBuilder();

      update.Set(FIELD_EXPIRATION, bsonExpiration);

      this.m_Writer.Update(query, update, UpdateFlags.None, WriteConcern.Unacknowledged);
    }


    /// <summary>
    ///   Returns session end candidates.
    /// </summary>
    /// <param name="application">
    ///   The name of the application.
    /// </param>
    /// <param name="signalTime">
    ///   The date and time the signal for processing expired sessions was generated, expressed as UTC.
    ///   <c>DateTime.Kind</c> should be <c>DateTimeKind.Utc</c>.
    /// </param>
    /// <param name="limit">
    ///   The limit of candidates.
    /// </param>
    /// <param name="candidates">
    ///   The collection of session end candidates.
    /// </param>
    /// <returns>
    ///   The number of session end candidates.
    /// </returns>
    internal int GetSessionEndCandidates([NotNull] string application, DateTime signalTime, int limit, [NotNull] IList<SessionEndCandidate> candidates)
    {
      Debug.ArgumentNotNull(application, "application");
      Debug.ArgumentNotNull(candidates, "candidates");
      Debug.Assert(limit > 0, "The limit argument is not valid.");

      BsonString bsonApplicationId = new BsonString(application);
      BsonDateTime bsonExpiration = new BsonDateTime(signalTime);

      IMongoQuery queryApplicationIdMatches = Query.EQ(FIELD_ID + "." + FIELD_APPLICATION, bsonApplicationId);
      IMongoQuery querySessionIsExpired = Query.LT(FIELD_EXPIRATION, bsonExpiration);
      IMongoQuery query = Query.And(queryApplicationIdMatches, querySessionIsExpired);
      IMongoSortBy sortBy = SortBy.Ascending(FIELD_EXPIRATION);

      MongoCursor<BsonDocument> cursor = this.m_Reader.FindAs<BsonDocument>(query);

      cursor.SetFields(FIELD_ID + "." + FIELD_SESSION_ID, FIELD_EXPIRATION);
      cursor.SetSortOrder(sortBy);
      cursor.SetLimit(limit);

      int result = 0;

      foreach (BsonDocument document in cursor)
      {
        var candidate = new SessionEndCandidate
        {
          Id = document[FIELD_ID][FIELD_SESSION_ID].AsString,
          Expiration = document[FIELD_EXPIRATION].AsBsonDateTime,
        };

        candidates.Add(candidate);
        result += 1;
      }

      return result;
    }



    /// <summary>
    /// Locks and returns an expired session item, if it has not been locked by a request or another session expiration handler.
    /// </summary>
    /// <param name="application">Application identifier for the item.</param>
    /// <param name="candidate">Descriptor for the session item.</param>
    /// <param name="lockCookie">Lock cookie to use when locking the item.</param>
    /// <returns>An instance of <see cref="SessionStateStoreData"/> if the method succeeds, <c>null</c> if the item has been removed or already locked by another party.</returns>
    [CanBeNull]
    internal SessionStateStoreData GetExpiredItemExclusive([NotNull] string application, [NotNull] SessionEndCandidate candidate, [NotNull] Sitecore.SessionProvider.SessionStateLockCookie lockCookie)
    {
      Sitecore.Diagnostics.Debug.ArgumentNotNull(application, "application");
      Sitecore.Diagnostics.Debug.ArgumentNotNull(candidate, "candidate");
      Sitecore.Diagnostics.Debug.ArgumentNotNull(lockCookie, "lockCookie");
      Assert.ArgumentNotNull(application, "application");
      Assert.ArgumentNotNull(candidate, "candidate");
      Assert.ArgumentNotNull(lockCookie, "lockCookie");

      SessionStateStoreData result = null;

      BsonString bsonLockCookie = new BsonString(lockCookie.Id);
      BsonDateTime bsonLockTimestamp = new BsonDateTime(lockCookie.Timestamp);
      BsonDateTime bsonExpiration = new BsonDateTime(DateTime.UtcNow.AddMinutes(2)); // session expiration handler adds 2 minutes to item timeout, hardcode.

      IMongoQuery querySessionIdMatches = this.BuildQueryBySessionIdentifier(application, candidate.Id);
      IMongoQuery queryExpirationUnchanged = Query.EQ(FIELD_EXPIRATION, candidate.Expiration);
      IMongoQuery query = Query.And(querySessionIdMatches, queryExpirationUnchanged);

      UpdateBuilder update = new UpdateBuilder();

      update.Set(FIELD_EXPIRATION, bsonExpiration);
      update.Set(FIELD_LOCK_COOKIE, bsonLockCookie);
      update.Set(FIELD_LOCK_TIMESTAMP, bsonLockTimestamp);

      FindAndModifyResult famr = this.m_Writer.FindAndModify(query, null, update, true, false);

      if (famr.Ok && (null != famr.ModifiedDocument))
      {
        result = Sitecore.SessionProvider.SessionStateSerializer.Deserialize(famr.ModifiedDocument[FIELD_DATA].AsByteArray);
      }

      return result;
    }


    /// <summary>
    /// Contains information about a candidate for session expiration handler.
    /// </summary>
    internal class SessionEndCandidate
    {
      /// <summary>
      /// Gets or sets the unique identifier of the session item.
      /// </summary>
      [NotNull]
      public string Id { get; set; }

      /// <summary>
      /// Gets or sets the current time of item expiration, as read by <see cref="GetSessionEndCandidates"/>.
      /// </summary>
      [NotNull]
      public BsonDateTime Expiration { get; set; }
    }
  }
}