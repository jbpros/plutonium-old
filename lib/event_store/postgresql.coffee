url         = require "url"
async       = require "async"
uuid        = require "node-uuid"
Event       = require "../event"
Snapshot    = require "../snapshot"
Base        = require "./base"
Profiler    = require "../profiler"
pg          = require("pg").native
defer       = require "../defer"
format      = require("util").format

class PostgresqlEventStore extends Base

  constructor: ({@uri, @logger}) ->
    throw new Error "Missing URI" unless @uri
    throw new Error "Missing logger" unless @logger

    @eventTableName    = "events"
    @snapshotTableName = "snapshots"

  createNewUid: (callback) ->
    uid = uuid.v4()
    callback null, uid

  initialize: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      done()
      callback null

  _handleError: (err, pgClient, pgCallback, callback) ->
    pgCallback(pgClient)
    callback(err)

  _dropEventTable: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "DROP TABLE IF EXISTS %s;"
      query = format query, @eventTableName

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback()

  _createEventTable: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "CREATE TABLE IF NOT EXISTS %s (
                id          SERIAL PRIMARY KEY,
                version     integer,
                name        varchar(255),
                data        text,
                entity_uid  varchar(255),
                timestamp   bigint,
                attachments text);"
      query = format query, @eventTableName

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback()

  _createIndexOnEventTable: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      indexName = "#{@eventTableName}_entity_uid_idx"

      query = "DROP INDEX IF EXISTS %s;
              CREATE INDEX ON %s (entity_uid);"
      query = format query, indexName, @eventTableName

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback()

  _dropSnapshotTable: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "DROP TABLE IF EXISTS %s;"
      query = format query, @snapshotTableName

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback()

  _createSnapshotTable: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "CREATE TABLE IF NOT EXISTS %s (
                id         SERIAL PRIMARY KEY,
                contents   text,
                entity_uid varchar(255),
                version    integer);"
      query = format query, @snapshotTableName

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback null

  _createIndexOnSnapshotTable: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      indexName = "#{@snapshotTableName}_entity_uid_idx"

      query = "DROP INDEX IF EXISTS %s;
              CREATE INDEX ON %s (entity_uid);"
      query = format query, indexName, @snapshotTableName

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback null

  _addAutoIncrementOnEventVersion: (callback) ->
    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "CREATE OR REPLACE FUNCTION events_version_auto_increment()
                  RETURNS trigger AS $$
                DECLARE
                  _rel_id constant int := 'events'::regclass::int;
                BEGIN
                  PERFORM pg_advisory_xact_lock(_rel_id);

                  SELECT  COALESCE(MAX(version) + 1, 1)
                  INTO    NEW.version
                  FROM    events
                  WHERE   entity_uid = NEW.entity_uid;

                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql STRICT;

                DROP TRIGGER IF EXISTS events_version_auto_increment on events;

                CREATE TRIGGER events_version_auto_increment
                  BEFORE INSERT ON events
                  FOR EACH ROW WHEN (NEW.version IS NULL)
                  EXECUTE PROCEDURE events_version_auto_increment();"

      client.query query, (err) =>
        return @_handleError(err, client, done, callback) if err?
        done()
        callback null

  destroy: (callback) ->
    callback null

  setup: (callback) ->
    async.series [
      (next) =>
        @logger.info "Postgres event store", "Dropping event table"
        @_dropEventTable next
      (next) =>
        @logger.info "Postgres event store", "Creating event table"
        @_createEventTable next
      (next) =>
        @logger.info "Postgres event store", "Creating index on event table"
        @_createIndexOnEventTable next
      (next) =>
        @logger.info "Postgres event store", "Adding auto increment on event table"
        @_addAutoIncrementOnEventVersion next
      (next) =>
        @logger.info "Postgres event store", "Dropping snapshot table"
        @_dropSnapshotTable next
      (next) =>
        @logger.info "Postgres event store", "Creating snapshot table"
        @_createSnapshotTable next
      (next) =>
        @logger.info "Postgres event store", "Creating index on snapshot table"
        @_createIndexOnSnapshotTable next
    ], callback

  iterateOverAllEvents: (eventHandler, callback) ->
    @_iterateOverEvents null, eventHandler, callback

  iterateOverEntityEventsAfterVersion: (entityUid, version, eventHandler, callback) ->
    @_iterateOverEvents "entity_uid='#{entityUid}' AND version > #{version}", eventHandler, callback

  iterateOverEntityEvents: (entityUid, eventHandler, callback) ->
    @_iterateOverEvents "entity_uid='#{entityUid}'", eventHandler, callback

  _iterateOverEvents: (params, eventHandler, callback) ->
    p = new Profiler "PostgresqlEventStore#_iterateOverEvents (db request)", @logger
    p.start()

    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query  = "SELECT * FROM %s"
      query += " WHERE %s" if params?
      query += " ORDER BY id ASC;"

      if params?
        query = format query, @eventTableName, params
      else
        query = format query, @eventTableName

      clientReceiver = client.query query

      clientReceiver.on "error", (err) =>
        p.end()
        return @_handleError(err, client, done, callback)

      clientReceiver.on "row", (row) =>
        @_instantiateEventFromRow row, (err, event) ->
          return @_handleError(err, client, done, callback) if err?

          eventHandler event, (err) ->
            return @_handleError(err, client, done, callback) if err?

      clientReceiver.on "end", (results) =>
        p.end()
        done()
        callback()

  findAllEvents: (options, callback) ->
    params = true
    order  = "ASC"

    @_find params, order, callback

  _find: (params, order, limit, callback) ->
    [limit, callback] = [null, limit] unless callback?

    p = new Profiler "PostgresqlEventStore#_find(db request)", @logger
    p.start()

    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query  = "SELECT * FROM %s WHERE %s ORDER BY id %s"
      query += " LIMIT %s" if limit?
      query += ";"

      if limit?
        query = format query, @eventTableName, params, order, limit
      else
        query = format query, @eventTableName, params, order

      clientReceiver = client.query query, (err, results) =>
        p.end()
        return @_handleError(err, client, done, callback) if err?
        done()
        @_instantiateEventsFromRows results.rows, (err, events) ->
          callback err, events

  _count: (params, callback) ->
    p = new Profiler "PostgresqlEventStore#_count(db request)", @logger
    p.start()

    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "SELECT COUNT(*) FROM %s WHERE %s;"
      query = format query, @eventTableName, params

      clientReceiver = client.query query, (err, results) =>
        p.end()
        return @_handleError(err, client, done, callback) if err?
        done()
        callback null, results.count

  findAllEventsByEntityUid: (entityUid, order, callback) ->
    [order, callback] = [null, order] unless callback?

    params = "entity_uid='#{entityUid}'"
    order  ?= "ASC"

    @_find params, order, callback

  countAllEventsByEntityUid: (entityUid, callback) ->
    params = "entity_uid='#{entityUid}'"

    @_count params, callback

  findSomeEventsByEntityUidBeforeVersion: (entityUid, version, eventCount, callback) ->
    params = "entity_uid='#{{entityUid}}' AND version <= #{version}"
    order  = "ASC"

    @_find params, order, eventCount, callback

  escapeString: (string) ->
    return "NULL" if string is null
    hasBackSlash = ~string.indexOf "\\"
    prefix = if hasBackSlash then "E" else ""
    string = string.replace /'/g, "''"
    string = string.replace /\\/g, "\\\\"
    prefix + "'" + string + "'"

  saveEvent: (event, callback) =>
    p = new Profiler "PostgresqlEventStore#saveEvent (db request)", @logger
    p.start()

    data        = {}
    attachments = {}

    for key, value of event.data
      if value instanceof Buffer
        attachments[key] = value
      else
        data[key] = value

    data        = JSON.stringify data
    attachments = JSON.stringify attachments

    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query = "INSERT INTO %s (name, entity_uid, timestamp, data, attachments) VALUES (%s, %s, %d, %s, %s);"
      query = format query, @eventTableName, @escapeString(event.name), @escapeString(event.entityUid), event.timestamp, @escapeString(data), @escapeString(attachments)

      clientReceiver = client.query query, (err, results) =>
        p.end()
        return @_handleError(err, client, done, callback) if err?
        done()
        callback null, event

  loadSnapshotForEntityUid: (uid, callback) ->
    p = new Profiler "PostgresqlEventStore#loadSnapshotForEntityUid (db request)", @logger
    p.start()

    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      query  = "SELECT * FROM %s WHERE %s;"
      params = "entity_uid='#{uid}'"

      query = format query, @snapshotTableName, params

      clientReceiver = client.query query, (err, results) =>
        p.end()
        return @_handleError(err, client, done, callback) if err?
        done()

        rawSnapshot = results.rows[0]
        snapshot    = null

        if rawSnapshot?
          snapshotAttributes =
            version   : rawSnapshot.version
            entityUid : rawSnapshot.entity_uid
            contents  : JSON.parse(rawSnapshot.contents)

          snapshot = new Snapshot snapshotAttributes

        callback null, snapshot

  saveSnapshot: (snapshot, callback) ->
    p = new Profiler "PostgresqlEventStore#saveSnapshot (db request)", @logger
    p.start()

    pg.connect @uri, (err, client, done) =>
      return @_handleError(err, client, done, callback) if err?

      version   = snapshot.version
      contents  = JSON.stringify(snapshot.contents)
      entityUid = snapshot.entityUid

      query = "WITH upsert AS (UPDATE %s SET version=%d, contents=%s WHERE entity_uid=%s RETURNING *) INSERT INTO %s (version, contents, entity_uid) SELECT %d, %s, %s WHERE NOT EXISTS (SELECT * FROM upsert);"
      query = format query, @snapshotTableName, version, @escapeString(contents), @escapeString(entityUid), @snapshotTableName, version, @escapeString(contents), @escapeString(entityUid)

      clientReceiver = client.query query, (err, results) =>
        p.end()
        return @_handleError(err, client, done, callback) if err?
        done()
        callback null

  _instantiateEventsFromRows: (rows, callback) ->
    events = []
    return callback null, events if rows.length is 0

    rowsQueue = async.queue (row, rowCallback) =>
      @_instantiateEventFromRow row, (err, event) ->
        return callback err if err?
        events.push event
        defer rowCallback
    , 1

    rowsQueue.drain = ->
      callback null, events

    rowsQueue.push rows

  _instantiateEventFromRow: (row, callback) ->
    data =
      uid         : row.id
      name        : row.name
      entityUid   : row.entity_uid
      data        : JSON.parse(row.data)
      timestamp   : row.timestamp
      version     : row.version

    @_loadAttachmentsFromRow row, (err, attachments) ->
      return rowCallback err if err?

      for attachmentName, attachmentBody of attachments
        data[attachmentName] = attachmentBody

      event = new Event data

      callback null, event

  _loadAttachmentsFromRow: (row, callback) ->
    attachments = {}
    for attachmentName, attachmentBody of JSON.parse(row.attachments)
      attachments[attachmentName] = attachmentBody.buffer

    callback null, attachments

module.exports = PostgresqlEventStore
