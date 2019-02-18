var express = require('express');
var app = express();
const fs = require('fs');

var socket = require('./socket.js');
var http = require('http').Server(app);
var io = require('socket.io')(http);
var s = new socket(io);
var routes = require('./routes.js');
routes.setSocket(s);
s.start();

app.use(express.static(__dirname + '/public'));
app.use('/', routes);

http.listen(3000, function () {
    console.log('listening on *:3000');
});
