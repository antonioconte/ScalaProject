var dataset = {
    "ranking_values": {
        "0": {},
        "1": {},
        "2": {},
        "3": {},
        "4": {},
        "5": {},
        "6": {},
        "7": {},
        "8": {},
        "9": {},
        "10": {}
    }
}
var selectComp = 2; //default: General 
function insertIter(iter, newData) {
    dataset["ranking_values"][iter] = sortByRank(newData);
}


//globals
var margin = { top: 10, right: 2, bottom: 10, left: 2 },
    width = 1450,
    height = 700 - margin.top - margin.bottom,
    header = 15,
    innerheight = height - header,
    linePadding = 15,
    dataset;

function buildChart(dataset) {
    dataset = dataset.ranking_values;

    var metricwidth = width / d3.values(dataset).length;
    // create the lines from the index values
    var pathfunction = function (obj) {
        var datavals = obj.data,
            dataLen = datavals.length,
            // new array to store the x/y pairs to build the 'plateaus'
            newArray = [];
        datavals.forEach(function (d, i) {
            var x1, y1, x2, y2;
            // if the value isn't null
            if (d !== null) {
                x1 = metricwidth * i + linePadding;
                y1 = header + (innerheight / obj.len) * (d + 1);
                x2 = metricwidth * i + metricwidth - linePadding;
                y2 = header + (innerheight / obj.len) * (d + 1);
                newArray.push({ x: x1, y: y1 });
                newArray.push({ x: x2, y: y2 });
            } else {
                // blank space in line if null value...
                x1 = y1 = x2 = y2 = null;
                newArray.push({ x: x1, y: y1 });
                newArray.push({ x: x2, y: y2 });
            }

        });

        var lineFunction = d3.svg.line()
            .defined(function (d) { return d.y != null; }) // no if null
            .x(function (d) { return d.x; })
            .y(function (d) { return d.y; })
        return lineFunction(newArray); // send back the line
    }

    var mouseOver = function (selectorclass) {
        d3.selectAll('.' + selectorclass + '_line:not(.line-click)')
            .classed('line-no-accent', false)
            .classed('line-accent', true)
        d3.selectAll('.' + selectorclass + '_text:not(.label-click)')
            .classed('label-no-accent', false)
            .classed('label-accent', true)
    }
    var mouseOut = function (selectorclass) {
        d3.selectAll('.' + selectorclass + '_line:not(.line-click)')
            .classed('line-accent', false)
            .classed('line-no-accent', true)
        d3.selectAll('.' + selectorclass + '_text:not(.label-click)')
            .classed('label-no-accent', true)
            .classed('label-accent', false)
    }
    var click = function (selectorclass) {
        d3.selectAll('.line-click')
            .classed('line-click', false)
            .classed('line-accent', false)
            .classed('line-no-accent', true)
        d3.selectAll('.label-click')
            .classed('label-click', false)
            .classed('label-accent', false)
            .classed('label-no-accent', true)
        d3.selectAll('.' + selectorclass + '_line')
            .classed('line-click', true)
        d3.selectAll('.' + selectorclass + '_text')
            .classed('label-click', true)
    }
    var rectSlide = function (idx) {
        var rect = d3.select("rect");
        rect.transition()
            .attr("x", (metricwidth * idx));
    }


    var svg = d3.select("#rankList")
        .append("svg")
        .attr("class", "viz")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom);

    //Draw the Rectangle
    var rectangle = svg.append("rect")
        .attr("x", 0)
        .attr("y", margin.top)
        .attr("rx", 10)
        .attr("ry", 10)
        .attr("width", metricwidth)
        .attr("height", height)
        .attr("stroke", "black")
        .attr("stroke-width", 1)
        .attr("fill", "lightyellow")

    // big nasty re-work of data to align results with technology, not measure as the data have it...
    var metriclist = d3.entries(dataset)[0].value
    var lookup = [];

    // loop first record to get technology list
    metriclist.forEach(function (rec, idx) {
        var o = {};
        o.metric = rec.id;
        o.data = [];
        lookup.push(o);
    })

    var metrics = d3.entries(dataset);

    metrics.forEach(function (record, index) {
        // get key by which to group
        var metric = record.key;
        // get array of values (this gives the loop the right limit)
        var vals = record.value;
        // lopp the results
        vals.forEach(function (rec, idx) {
            // loop the tech list lookup
            lookup.forEach(function (r, i) {
                // if we have a match...
                if (r.metric == rec.id) {
                    // make sure it's not null...
                    if (rec.rank != null) {
                        // add the index number to the lokup -- this gives us the rank order, as the original data are loaded ascending
                        lookup[i].data[index] = idx;
                        // if null...
                    } else {
                        // assign null val to lookup, to allow line to drop off
                        lookup[i].data[index] = null;
                    }

                }
            })
        })
    })


    // build the lines
    var paths = svg
        .selectAll("paths")
        // bind lookup to lines
        .data(lookup)
        .enter()
        .append("path")
        .attr("class", function (d) { return d.metric + '_line' }) // use class attr to add login to class assignment
        .classed("line", true) // add additional classes with classed
        .classed("line-no-accent", true)
        // call path function on data, passing in array length for y measure
        .attr("d", function (d, i) { return pathfunction({ len: lookup.length, data: d.data }) })
        .on("mouseover", function (d) { mouseOver(d.metric) })
        .on("mouseout", function (d) { mouseOut(d.metric) })
        .on("click", function (d) { click(d.metric) })

    // pull put keys only for headers
    var keys = d3.keys(dataset);

    var headerText = svg
        .selectAll(".headertext")
        // bind data
        .data(keys)
        .enter()
        .append("text")
        .attr("class", "headertext")
        .attr("x", function (key, i) {
            return metricwidth * i + linePadding;
        })
        .attr("y", function (key, i) {
            return 30;
        })
        // .attr("font-size", "16px")
        // .attr("fill", "black")
        .text(function (key, i) {
            return key;
        })
        .on("click", function (d, i) {
            rectSlide(i);
        })
    var textGroup = svg
        .selectAll("textgroup")
        .data(d3.entries(dataset))
        .enter()
        .append("g");

    textGroup
        .each(function (record, i) {
            var metric = d3.select(this)
                .selectAll("text")
                .data(record.value)
                .enter()
                .append("text")
                .attr('class', function (d, idx) {
                    return d.id + '_text';
                })
                .classed('label', true)
                .classed("label-no-accent", true)
                .attr("x", function (d, idx) {
                    if (d) {
                        return metricwidth * i + linePadding;
                    }
                })
                .attr("y", function (d, idx) {
                    if (d) {
                        return header + (innerheight / record.value.length) * (idx + 1) - 5;
                    }
                })
                .text(function (d, idx) {
                    var rank = idx + 1;
                    if (d.rank != null) {
                        return '#' + String(rank) + ' ' + d.id + ' (' + String(Math.round(d.rank * 100) / 100) + ')'
                    }
                })
                .on("mouseover", function (d) { mouseOver(d.id) })
                .on("mouseout", function (d) { mouseOut(d.id) })
                .on("click", function (d) { click(d.id) })
        })
}