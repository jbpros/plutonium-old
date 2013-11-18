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

    request.on "response", (response) ->
      processResponse response, (err, data) ->
        return callback err if err?
        callback null, data.uid

    stream.end()

  executeCommand: (command, callback) ->
    logger = @logger
    commandName = command.getName()
    commandArgs = command.serialize()
    logger.log "CommandBusClient", "sending command \"#{commandName}\" to localhost:#{@port}..."

    request = @_makeRequest path: "/commands"
    stream  = request.stream

    request.on "error", (err) ->
      callback err

    request.on "response", (response) ->
      processResponse response, callback

    stream.write "Content-Disposition": "form-data; name=\"name\"", commandName

    for arg in args
      headers =
        "Content-Disposition": "form-data; name=\"args[]\""
        "Content-Type": "application/json"
      if not arg? or (not Buffer.isBuffer(arg) and not arg.pipe?)
        arg = null if arg is undefined
        stream.write headers, JSON.stringify arg
      else
        headers["Content-Type"] = "application/octet-stream"
        stream.write headers, arg

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

processResponse = (response, callback) ->
  logger = @logger
  data   = ""

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
      error = new Error "Remote command bus error"
      error.message = data.error || data
      error.response = response
      logger.error "CommandBusClient", "command <#{commandName}> failed remotely: #{error.message}"
      callback error

module.exports = CommandBusClient
