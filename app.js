var express = require('express');
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);
const { spawn  } = require('child_process');
const PORT = 8080;
const child = spawn('scala',['main.scala','ciaoooo']);

function isBlank(str) {
  return (!str || /^\s*$/.test(str));
}
child.stdout.on('data', (data) => {
  if(!isBlank(data)) console.log(`OUTPUT from SCALA: ${data}`);
});

child.stderr.on('data', (data) => {
  console.log(`ERROR from SCALA: ${data}`);
});
 
app.use(express.static(__dirname + '/public'));

// ROUTING
app.get('/', function (req, res) {
  res.sendFile('index.html');
});

io.on('connection', function(socket){
    console.log('Conessione con il client stabilita..');
    // SERVER manda msg ciao al client tramite un evento DATA
    //io.emit('data', 'ciao');
    //quando il client emette un evento di tipo eventType viene intercettato
    socket.on('provUpdate', (msg) => {
        console.log(msg);
        io.emit('provData',msg)
        
    });
});

http.listen(PORT,'localhost', function(){
  console.log('listening on '+http.address().address+ ':' + http.address().port);
});
