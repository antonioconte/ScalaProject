var rMax = 2, rMin = 1

/*PER DEBUG */
// draw()
/**/
function drawGraph(nodes_data, links_data) {
    insertIter("0",nodes_data)

    var svg = d3.select("svg"),
        width = +svg.attr("width"),
        height = +svg.attr("height");

    var simulation = d3.forceSimulation().nodes(nodes_data);

    var link_force = d3.forceLink(links_data).id(function (d) { return d.id; }).distance(100);
    var charge_force = d3.forceManyBody().strength(-1000);
    var center_force = d3.forceCenter(width / 2, height / 2);

    simulation
        .force("charge_force", charge_force)
        .force("center_force", center_force)
        .force("links", link_force)
        .on("tick", tickActions);

    var g = svg.append("g").attr("class", "everything");

    if(selectComp == 2) {
        //caso General implica l'orientamento
        svg.append('defs').append('marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '-0 -5 10 10')
        .attr('refY', 0)
        .attr('refX', 30)
        .attr('orient', 'auto')
        .attr('markerWidth', 5)
        .attr('markerHeight', 5)
        .attr('xoverflow', 'visible')
        .append('svg:path')
        .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
        .attr('fill', '#999')
        .style('stroke', 'none');

    }
    
    var link = g.append("g")
        .attr("class", "links")
        .selectAll("line")
        .data(links_data)
        .enter()
        .append("line")
        .attr("stroke-width", 2)
        .attr('marker-end', 'url(#arrowhead)');

    var node = g.append("g")
        .attr("class", "nodes")
        .selectAll("circle")
        .data(nodes_data)
        .enter()
        .append("circle")
        .attr("id", (d) => { return d.id })
        .attr("rank", (d) => { return d.rank })
        .attr("r", getRadius)
        .attr("fill", (d) => getColor(d.rank));

    node.on("mouseover", function (d) {
        document.getElementById("userSelected").innerText = d.id
        document.getElementById("rankUserSelected").innerText = $("#" + d.id).attr("rank")
        console.log(d.id, $("#" + d.id).attr("rank"))
    });

    node.on("click", function (d) {

    });





    var drag_handler = d3.drag()
        .on("start", drag_start)
        .on("drag", drag_drag);
    drag_handler(node);

    var zoom_handler = d3.zoom()
        .on("zoom", zoom_actions);
    zoom_handler(svg);



    // g.append("svg:g").

    /** Functions **/
    function getRadius(d) {
        var r = (((parseFloat(d.rank) + 1) * rMax / 2) + rMin)
        return 20
        // return parseInt(r)
    }

    function drag_start(d) {
        if (!d3.event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function drag_drag(d) {
        d.fx = d3.event.x;
        d.fy = d3.event.y;
    }




    function zoom_actions() {
        g.attr("transform", d3.event.transform)
    }

    function tickActions() {
        //update circle positions each tick of the simulation
        node
            .attr("cx", function (d) { return d.x; })
            .attr("cy", function (d) { return d.y; });

        //update link positions
        link
            .attr("x1", function (d) { return d.source.x; })
            .attr("y1", function (d) { return d.source.y; })
            .attr("x2", function (d) { return d.target.x; })
            .attr("y2", function (d) { return d.target.y; });
    }

}

function getColor(rank) {
    // [1.75,2] [1.25,1.75] [0.75,1.25] [0.25,0.75] [0,0.25]
    var tmp = parseFloat(rank) + 1.0
    if (tmp >= 1.8) return "#038105"
    else if (tmp >= 1.6 && tmp < 1.8) return "#21C903"
    else if (tmp >= 1.4 && tmp < 1.6) return "#7AF202"
    else if (tmp >= 1.2 && tmp < 1.4) return "#B6F803"
    else if (tmp >= 1.0 && tmp < 1.2) return "#F6FF05"
    else if (tmp >= 0.8 && tmp < 1.0) return "#F9CE03"
    else if (tmp >= 0.6 && tmp < 0.8) return "#F49602"
    else if (tmp >= 0.4 && tmp < 0.6) return "#EF5F01"
    else if (tmp >= 0.2 && tmp < 0.4) return "#EA2B00"
    else return "#E50000"
}

