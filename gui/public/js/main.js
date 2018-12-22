const long = 10.9836100
const lat = 44.5444000
const accessToken = 'pk.eyJ1IjoiYW50b25pb2NvbnRlIiwiYSI6ImNqcHY5eHM2azBwZzA0M3NhbGowZGt6enYifQ.FVQXLKZid6PgBU1PvWmWUQ';
var map = L.map('map').setView([lat,long], 8);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {
//id: 'mapbox.satellite',   
    id: 'mapbox.outdoors', 
    accessToken: accessToken,
    maxZoom:10,
    minZoom: 8
}).addTo(map);

function highlightFeature(e) {
    var layer = e.target;
    layer.setStyle({
        weight: 2,
        dashArray: '--',
        fillOpacity: 0.8
    });

    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }
    info.update(layer.feature.properties);
}

function resetHighlight(e) {
    geojson.resetStyle(e.target);
    info.update();
}

function onEachFeature(feature, layer) {
    layer.on({
      /*   mouseover: highlightFeature,
        mouseout: resetHighlight, */
        click: selectProv
    });
}

let provSel = null;
function selectProv(e){
    if(provSel) resetHighlight(provSel)
    highlightFeature(e) 
    provSel = e;
    let prov = e.target.feature.properties
    console.log(prov);
    socket.emit('provUpdate',prov);
}

function style(feature) {
    return {
        fillColor: '#800026',
        weight: 1,
        color: 'white',
        dashArray: '5',
        fillOpacity: 0.3
    };
}
var geojson = L.geoJson(all,{style: style, onEachFeature: onEachFeature}).addTo(map);
var info = L.control();

info.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'info'); // create a div with a class "info"
    this.update();
    return this._div;
};

info.update = function (props) {
    let str = props ? '</b>' + props.NOME_PRO : ''
    this._div.innerHTML = '<span>'+ str+'</span>';
}


info.addTo(map);


var socket = io();

socket.on('provData', (msg) => {
    console.log(msg)
});