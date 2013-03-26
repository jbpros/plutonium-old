http       = require "http"
url        = require "url"
formidable = require "formidable"

class CommandBusServer

  JSON_MEDIA_TYPE_REGEXP = /^application\/(.+\+)?json$/i

  constructor: ({@commandBus, @logger}) ->
    throw new Error "Missing command bus" unless @commandBus?
    throw new Error "Missing logger" unless @logger?

    @server = http.createServer (req, res) =>

      path = url.parse(req.url).path
      method = req.method
      if path is "/commands"
        if method is "POST"
          @_handleCommand req, res
        else
          code 405, res

      else if path is "/uids"
        if method is "POST"
          @_handleCreateNewUid req, res
        else
          code 405, res
      else
        code 404, res

  listen: (port) ->
    @server.listen port
    @logger.info "CommandBusServer", "listening on port #{port}..."

  close: (callback) ->
    @server.close callback

  _handleCreateNewUid: (req, res) ->
    @commandBus.createNewUid (err, uid) ->
      if err?
        djump res, 500, error: err
      else
        djump res, 200, uid: uid

  _handleCommand: (req, res) ->
    logger = @logger
    commandName = null
    args = []

    form = new formidable.IncomingForm();

    form.onPart = (part) ->
      data = ""

      part.on "data", (chunk) ->
        data += chunk

      part.on "end", ->
        contentType = part.headers["content-type"]
        if contentType and contentType.match JSON_MEDIA_TYPE_REGEXP
          data = JSON.parse data

        if part.name is "name" # command name
          commandName = data
        else if part.name is "args[]"
          args.push data

    form.parse req, (err, fields, files) =>
      if err?
        logger.warning "CommandBusServer", "received command failed (#{err})"
        return djump res, 400, error: err
      unless commandName?
        logger.warning "CommandBusServer", "missing command name"
        return djump res, 400, error: new Error "Missing command name"

      logger.log "CommandBusServer", "start command \"#{commandName}\""
      @commandBus.executeCommand commandName, args..., (err) ->
        if err?
          logger.alert "CommandBusServer", "error while executing command (#{err})"
          djump res, 500, error: err
        else
          logger.log "CommandBusServer", "command \"#{commandName}\" started successfully"
          djump res, 202

djump = (res, code, obj) ->
  res.statusCode = code
  if obj?
    res.setHeader "Content-Type", "application/vnd.djump-command-bus+json"
    res.end JSON.stringify obj
  else
    res.end()

module.exports = CommandBusServer