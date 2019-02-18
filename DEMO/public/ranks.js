
function sortByProperty(property) {
    return function (x, y) {
        return ((x[property] === y[property]) ? 0 : ((x[property] < y[property]) ? 1 : -1));
    };
};
function sortByRank(data){
    // console.log(data);
    return data.sort(sortByProperty('rank'));
}

/* Debug */
/*
var data = [{"id":"T","rank":0.5000000596046448},{"id":"L","rank":0.40673306584358215}]
insertIter(0,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(1,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(2,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(3,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(4,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(5,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(6,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(7,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(8,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(9,data)
data = [{"id":"T","rank":-1},{"id":"L","rank":0.40673306584358215}]
insertIter(10,data)
buildChart(dataset) */

