const routes = require('express').Router();
const { spawn } = require('child_process');

var socket = null //Condivisa con app.js
const loadingIcon = "<i class='fas fa-spinner fa-spin'></i>"
var worker=3,core=2;
routes.get('/', (req, res) => {
    res.sendFile('index.html');

});
/* LOCAL */
routes.get('/startLocalProduct', (req, res) => {
    res.status(200).json({ msg: 'START RUNNING SCRIPT [ LOCAL ] >>>> PRODUCT' });
    runJobLocal(1)
})
routes.get('/startLocalGeneral', (req, res) => {
    res.status(200).json({ msg: 'START RUNNING SCRIPT [ LOCAL ] >>>> GENERAL' });
    runJobLocal(2)
})
routes.get('/startClusterProduct', (req, res) => {
    res.status(200).json({ msg: 'START RUNNING SCRIPT [ CLUSTER ] >>>> PRODUCT' });
    runJobCluster(1)
})
routes.get('/startClusterGeneral', (req, res) => {
    res.status(200).json({ msg: 'START RUNNING SCRIPT [ CLUSTER ] >>>> GENERAL' });
    runJobCluster(2)
})

function runJobLocal(type) {
        var run;
        if (type == 1) run = spawn('./Script/Local/args.sh', ['Product']);
        else run = spawn('./Script/Local/args.sh', ['General']);

        var nodes = false, links = false;
        var nodesInitial, linksInitial;
        run.stdout.on('data', (data) => {
            data = data.toString('utf8');
            if (data.includes("#nodesInitial#")) {
                data = data.split("#nodesInitial#")[1];
                var start = data.indexOf("[");
                var end = data.indexOf("]") + 1;
                data = data.substr(start, end);
                var jsonData = JSON.parse(data);
                if (links) socket.emit("initGraph", { nodes: jsonData, links: linksInitial, iter: 0 })
                else {
                    nodesInitial = jsonData
                    nodes = true;
                }
            } else if (data.includes("#linksInitial#")) {
                data = data.split("#linksInitial#")[1];
                var start = data.indexOf("[");
                var end = data.indexOf("]") + 1;
                data = data.substr(start, end);
                var jsonData = JSON.parse(data);
                if (nodes) socket.emit("initGraph", { nodes: nodesInitial, links: jsonData, iter: 0 })
                else {
                    linksInitial = jsonData
                    links = true;
                }
            } else if (data.includes("#nodesIter")) {
                data = data.split("#[");
                var str = "[" + data[1]
                console.log(str)
                var iter = parseInt(data[0].split("Iter")[1])
                var start = str.indexOf("[");
                var end = str.indexOf("]") + 1;
                str = str.substr(start, end);
                var jsonData = JSON.parse(str);
                socket.emit("updateGraph", { nodes: jsonData, iter: iter })
            }
            else {
            //     console.log(">SHELL: ",data)
            //     socket.emit('shellMsg', data)
            }
        });

        run.stderr.on('data', (data) => {
            console.log(`stderr: ${data}`);
        });


}
function runJobCluster(type){
    var run;
    if (type == 1) run = spawn('./Script/gcpScript/8.new_dataproc-run-job.sh', [worker,core,'Product']);
    else run = spawn('./Script/gcpScript/8.new_dataproc-run-job.sh', [worker,core,'General']);

    var nodes = false, links = false;
    var nodesInitial, linksInitial;
    run.stdout.on('data', (data) => {});

    run.stderr.on('data',(data) => {
        data = data.toString('utf8')
        if (data.includes("#nodesInitial#")) {
            str = data.split("#nodesInitial#")[1];
            var start = str.indexOf("[");
            var end = str.indexOf("]") + 1;
            str = str.substr(start, end);
            console.log("INIT NODES: ",str)
            var jsonData = JSON.parse(str);
            if (links) socket.emit("initGraph", { nodes: jsonData, links: linksInitial, iter: 0 })
            else {
                nodesInitial = jsonData
                nodes = true;
            }
        }
        if (data.includes("#linksInitial#")) {
            str = data.split("#linksInitial#")[1];
            var start = str.indexOf("[");
            var end = str.indexOf("]") + 1;
            str = str.substr(start, end);
            console.log("INIT LINKS: ",str)
            var jsonData = JSON.parse(str);
            if (nodes) socket.emit("initGraph", { nodes: nodesInitial, links: jsonData, iter: 0 })
            else {
                linksInitial = jsonData
                links = true;
            }
        }
        if (data.includes("#nodesIter")) {
            /*è un casino da spiegare!!!*/
            var startNodes = data.indexOf("#nodesIter")
            nodesIterStr= data.substr(startNodes,data.length)
            nodesIterStr = nodesIterStr.substr(0,nodesIterStr.indexOf("]")+1)
            nodesIterStr= nodesIterStr.split("#[");
            var str = "[" + nodesIterStr[1]
            var iter = parseInt(nodesIterStr[0].split("Iter")[1])
            var start = str.indexOf("[");
            var end = str.indexOf("]") + 1;

            str = str.substr(start, end);
            console.log("ITER " + iter + " - NODES: ",str)
            var jsonData = JSON.parse(str);
            socket.emit("updateGraph", { nodes: jsonData, iter: iter })
        }
        if (data.includes("Tempo di calcolo:")){
            let time = data.split("\n")[2];
            console.log("-> ", time);
            socket.emit("END_COMPUTATION", time)
        }
        if(data.includes("finished successfully")){
            const shDownload = spawn('./Script/gcpScript/download_result.sh');
            shDownload.stderr.on("data", (data) => {
                socket.emit("DOWNLOAD_URL", data)
            })
        }
        //todo downloadFIle
        console.log(data)
    });


}

/*GOOGLE CLOUD PLATFORM */
routes.get('/makeBucket', (req, res) => {
    res.json({ msg: 'Creando il bucket ...'})
    const shMakeBucket = spawn('./Script/gcpScript/4.new_gcs-make-bucket.sh');
    shMakeBucket.stdout.on('data', (data) => {
        data = data.toString('utf8');
        if (data.includes("#:")) {
            let str = data.split("#:")[1]
            socket.emit('shellMsgStatus', str)
            console.log(">", str);
        }
    });
    shMakeBucket.stderr.on('data', (data) => {
        data = data.toString('utf8');
        let perc = data.match(/[0-9]+%/gm)
        if (perc != null) socket.emit('shellMsgStatus', "Uploading Files: " + perc[0] + " " + loadingIcon)
    });
});
routes.get('/deleteCluster',(req,res) => {

    res.status(200).json({ msg: 'Removing Cluster 1M ' + worker + "W " + core + "C " + loadingIcon });
    const delCluster = spawn('./Script/gcpScript/3.new_dataproc-delete-cluster.sh', [worker,core]);
    delCluster.stderr.on('data', (data) => {
        data = data.toString("utf8")
        console.log(data);
    });


    delCluster.stdout.on('data', (data) => {
        data = data.toString("utf8")
        socket.emit("CLUSTER_DELETED",data)
    })
})
routes.get('/makeCluster', (req, res) => {
    const typeCluster = req.query.typeCluster;
    if(typeCluster == "7W"){
        worker = 7
        core = 1
    } else {
        worker = 3
        core = 2
    }
    res.status(200).json({ msg: 'Creando il Cluster 1M ' + worker + "W " + core + "C " + loadingIcon });
    const makeCluster = spawn('./Script/gcpScript/2.new_dataproc-create-cluster.sh', [worker,core]);
    makeCluster.stdout.on('data', (data) => {
        data = data.toString('utf8')
        console.log(data)
        if (data.includes("#:Cluster")) socket.emit('shellMsgStatus', data.split("#:")[1])
    })
    makeCluster.stderr.on('data', (data) => {
        data = data.toString('utf8')
        if(data.length > 5) console.log("> ",data)

    })

});




module.exports = routes
module.exports.setSocket = (s) => { socket = s }
