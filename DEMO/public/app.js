var socket = io();
var numIter = -1;
const btnRunJobLocal = document.getElementById("runJobLocal")
const btnSearchUser = document.getElementById("btnSearchUser")
const btnMakeBucket = document.getElementById("makeBucket")
const btnsCreateCluster = document.getElementsByClassName("makeCluster")
const btnRunJobCluster = document.getElementById("runClusterJob")
const btnRemoveCluster = document.getElementById("removeCluster")
btnRunJobCluster.addEventListener('click', startJobCluster)
btnRemoveCluster.addEventListener('click', removeCluster)
btnRunJobLocal.addEventListener('click', startJobLocal)
btnSearchUser.addEventListener('click', searchUserLigthing)
btnMakeBucket.addEventListener('click', createBucket)
Array.from(btnsCreateCluster).forEach(function(element) {
    element.addEventListener('click', makeCluster);
  });

/* GRAPH UPDATE */
var refreshGraph = null
function updateNode(nodes_data, links, iter) {
    var statusText = "Iterazione: ";
    if (parseInt(iter) == 0) {
        drawGraph(nodes_data, links);
        statusText += iter
        document.getElementById("numIter").innerText = statusText;
    }
    else {
        if (parseInt(iter) == 10) statusText += iter + " - Fine"
        else statusText += iter
        document.getElementById("numIter").innerText = statusText;
        insertIter(iter, nodes_data)
        for (let i = 0; i < nodes_data.length; i++) {
            let currentRank = parseFloat(nodes_data[i].rank); //nuovo valore del rank
            $("#" + nodes_data[i].id)
                .attr("rank", currentRank.toString())
                // .attr("r",(((currentRank+1) * rMax/2)+rMin)) //aggiorna dimensione
                .attr("fill", getColor(currentRank));        //aggiorna di conseguenza il colore
        }
        if (parseInt(iter) == 10) buildChart(dataset);
    }

}

function removeCluster(){
    
    $.get("/deleteCluster", function (data, status) {
        $(".infoStatus").html(data.msg);
    });
}

function startJobLocal() {

    selectComp = $("#selectComp").val()
    if (selectComp == 0) {
        alert("seleziona modalità")
        return
    }
    $("#typeComp").html("")
    $(".loading").removeClass("hide")
    document.getElementById("numIter").innerText = "Starting...";
    $("#graph").html("") //reset view graph
    var url = "/startLocalGeneral"
    if (selectComp == 1) url = "/startLocalProduct"


    $.get(url, function (data, status) {
        console.log(data.msg);
    });
}



function startJobCluster() {
    selectComp = $("#selectComp").val()
    if (selectComp == 0) {
        alert("seleziona modalità")
        return
    }
    $("#typeComp").html("")
    $(".loading").removeClass("hide")
    document.getElementById("numIter").innerText = "Starting...";
    $("#graph").html("") //reset view graph
    var url = "/startClusterGeneral"
    if (selectComp == 1) url = "/startClusterProduct"


    $.get(url, function (data, status) {
        console.log(data.msg);
    });
}

function createBucket(){
    $.get('/makeBucket', function (data, status) {
        $(".infoStatus").html(data.msg);
    });
}

function makeCluster(e){
    const typeCluster = $(this).attr("value");
    $.get('/makeCluster', {typeCluster}, function (data, status) {
        console.log(data.msg)
        $(".infoStatus").html(data.msg)
    });

}

/* EVENT FROM SERVER */
// socket.on('shellMsg', function (msg) {
//     if (msg.includes("Prodotto")) $("#typeComp").html("<b>Mode: Computazione per Prodotto</b>")
//     else if (msg.includes("Generale")) $("#typeComp").html("<b>Mode: Computazione Generale</b>")
// });

socket.on('updateGraph', function (data) {
    // console.warn("updateGraph", data.nodes, data.iter)
    $(".loading").addClass("hide")
    updateNode(data.nodes, [], data.iter)
});

socket.on('initGraph', function (data) {
    // console.warn("initGraph", data.nodes, data.links, data.iter)
    $(".loading").addClass("hide")
    updateNode(data.nodes, data.links, data.iter)
});

socket.on('shellMsgStatus',function (data) {
    $(".infoStatus").html(data)
});

socket.on("END_COMPUTATION", function(data){
    $("#numIter").html(data)
});

socket.on("DOWNLOAD_URL", function(data){    
    $(".infoStatus").html("<a href='result.txt'>Download result</a>")
})

socket.on("CLUSTER_DELETED", function (data) {    
    $(".infoStatus").html(data)
})


/* HIGHLIGHTS */
function searchUserLigthing() {
    var idUser = $("#searchUser").val().toLowerCase()
    if (idUser == "") {
        lightOffNode()
        return;
    }
    idUser = idUser.charAt(0).toUpperCase() + idUser.substr(1);
    $("#searchUser").val("");
    lightOnNode(idUser)
}
var lightOnNode = function (id) {
    lightOffNode()
    d3.select("#" + id).attr("class", "selected")
}
var lightOffNode = function () {
    d3.selectAll(".selected").classed('selected', false)
}

