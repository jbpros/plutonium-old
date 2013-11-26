http      = require "http"
multipart = require "multipart"
crypto    = require "crypto"

JSON_MEDIA_TYPE_REGEXP = /^application\/(.+\+)?json$/i

class CommandBusClient

  constructor: ({@port, @logger}) ->
    throw new Error "Missing port" unless @port?
    throw new Error "Missing logger" unless @logger?

  createNewUid: (callback) ->
    logger = @logger
    logger.log "CommandBusClient", "creating UID from localhost:#{@port}..."

    request = @_makeRequest path: "/uids"
    stream  = request.stream

    request.on "error", (err) ->
      callback err

    request.on "response", (response) =>
      @_processResponse response, (err, data) ->
        return callback err if err?
        callback null, data.uid

    stream.end()

  executeCommand: (command, callback) ->
    logger = @logger
    commandName = command.getName()
    payload = command.getPayload()
    logger.log "CommandBusClient", "sending command \"#{commandName}\" to localhost:#{@port}..."

    request = @_makeRequest path: "/commands"
    stream  = request.stream

    request.on "error", (err) ->
      callback err

    request.on "response", (response) =>
      @_processResponse response, callback

    stream.write "Content-Disposition": "form-data; name=\"name\"", commandName

    for property, data of payload
      headers =
        "Content-Disposition": "form-data; name=\"payload.#{property}\""
        "Content-Type": "application/json"
      if not data? or (not Buffer.isBuffer(data) and not data.pipe?)
        data = null if data is undefined
        stream.write headers, JSON.stringify data
      else
        headers["Content-Type"] = "application/octet-stream"
        stream.write headers, data

    stream.end()

  _makeRequestStream: ->
    boundaryPrefix = "DjumpBoundary-#{crypto.pseudoRandomBytes(16).toString 'base64'}"
    stream = multipart.createMultipartStream prefix: boundaryPrefix
    stream

  _makeRequest: ({path})->
    stream = @_makeRequestStream()
    options =
      port: @port
      host: "localhost"
      path: path
      method: "POST"
      headers: "Content-Type": "multipart/mixed; boundary=#{stream.boundary}"
    request = http.request options
    stream.pipe request
    request.stream = stream
    request

  _processResponse: (response, callback) ->
    data = ""
    logger = @logger

    response.on "data", (chunk) ->
      data += chunk

    response.on "end", ->
      succeeded = response.statusCode >= 200 and response.statusCode < 300
      contentType = response.headers["content-type"]
      if contentType and contentType.match JSON_MEDIA_TYPE_REGEXP
        data = JSON.parse data

      if succeeded
        callback null, data
      else
        try data = JSON.parse data
        error = data.error
        logger.error "CommandBusClient", "command failed remotely: #{error.stack || error.message || error}"
        callback error

module.exports = CommandBusClient
