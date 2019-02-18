class Socket {
    constructor(io) {
        this.io = io;
        this.count = 0
    }

    start() {
        this.io.on('connection', function (socket) {
            // console.log('a user connected');
            socket.on('disconnect', function () {
                // console.log('user disconnected');
            });

            socket.on('msgClient', function (data) {
                console.log('msg from client: ' + data);
            });
        });
    }

    emit(event, data) {
        console.log("> EventEmit: " + event);
        this.io.emit(event,data)
    }


}

module.exports = Socket