'use strict';

const crypto = require('crypto');
const iso8601 = require('iso8601');
const fs = require('fs');
const args = process.argv.slice(2)
var reporte_path = args[0]
var user = args[0]
var secret = args[1]

function getWsseHeader(user, secret) {
  let nonce = crypto.randomBytes(16).toString('hex');
  let timestamp = iso8601.fromDate(new Date());

  let digest = base64Sha1(nonce + timestamp + secret);
  var datos = "{\"username\":\"" + user + "\",\"digest\":\"" + digest + "\",\"nonce\":\"" + nonce + "\",\"created\":\"" + timestamp + "\"}"
  fs.writeFile('x-wsse.json', datos, function (err) {
    if (err) 
        return console.log(err);
});
  return `UsernameToken Username="${user}", PasswordDigest="${digest}", Nonce="${nonce}", Created="${timestamp}"`
};

function base64Sha1(str) {
  let hexDigest = crypto.createHash('sha1')
    .update(str)
    .digest('hex');

  return new Buffer(hexDigest).toString('base64');
};

getWsseHeader(user, secret)