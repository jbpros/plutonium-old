url = require "url"
http = require "http"

request = (options, parseJson, callback) ->
  [parseJson, callback] = [false, parseJson] unless callback?

  req = http.request options, (res) ->
    res.on "error", (err) ->
      callback err

    response = ""
    res.on "data", (chunk) ->
      response += chunk

    res.on "end", ->
      response = JSON.parse response if parseJson?
      callback null, response

  req.on "error", (err) ->
    callback err

  if options.payload? and (options.method is "POST" or options.method is "PUT")
    body = JSON.stringify options.payload
    req.write(body) 

  req.end()

get = (options, parseJson, callback) ->
  request options, parseJson, callback

post = (options, parseJson, callback) ->
  options.method = "POST"
  request options, parseJson, callback

put = (options, parseJson, callback) ->
  options.method = "PUT"
  request options, parseJson, callback

del = (options, parseJson, callback) ->
  options.method = "DELETE"
  request options, parseJson, callback

module.exports =
  get: get
  post: post
  put: put
  del: del