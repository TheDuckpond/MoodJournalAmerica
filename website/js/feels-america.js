/* Javscript File for America's Front Page 
 * Based on twitter-project.js with additions and modifications
 * @authored malam 7 July 2014, habdulkafi, some code adapted from twitter-project.js rpetchler 7/2013
 * Changelog:
 * 16 July 2014 - mansoor - added function to get date
 */

var dataDirectory = "/static/data/twitter_project/tmp_data_test/", // note that this has temporarily been changed
    dateFormat = d3.time.format("%x"),
    datetimeFormat = d3.time.format("%x %-I %p"),
    commaFormat = d3.format(",");

var congress, us;  // Geodata globals.

function getCurDate(){
  // probably a cleaner way...
  var mydate=new Date();
  var year=mydate.getYear();
  if (year < 1000)
    year+=1900;
  var day=mydate.getDay();
  var month=mydate.getMonth();
  var daym=mydate.getDate();
  if (daym<10)
    daym="0"+daym;
  var dayarray= new Array("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday");
  var montharray= new Array("January","February","March","April","May","June","July","August","September","October","November","December");
// never ever ever use document.write or anything like that.
document.getElementById('curDate').innerHTML = "<h5> Entry for "+dayarray[day]+", "+montharray[month]+" "+daym+", "+year+".</h5>";
}

/* Function for printing out movie statement */
function movieLine(filename,id){
  d3.csv(dataDirectory+ filename, function(error, data) {
          document.getElementById("movie-line").innerHTML = "<h4> If America made a movie today, it would star "+data[0].actors+","+data[1].actors+" and "+data[2].actors+". It would be rated "+data[0].rating+" and would have the following plot line";
        })};


/***** Function for line chart ******/
function lineWithFocusChart(filename, id, format) {
  d3.json(dataDirectory + filename, function(data) {
    nv.addGraph(function() {
      var chart = nv.models.lineWithFocusChart()
          .margin({top: 30, right: 40, bottom: 30, left: 60})
          .margin2({top: 0, right: 40, bottom: 30, left: 60})
          .x(function(d) { return d[0] })
          .y(function(d) { return d[1] });

      chart.xAxis
          .showMaxMin(false)
          .staggerLabels(true)
          .tickFormat(function(d) { return format(new Date(d)) });
      chart.x2Axis
          .showMaxMin(false)
          .staggerLabels(true)
          .tickFormat(function(d) { return format(new Date(d)) });

      chart.yAxis
          .showMaxMin(false)
          .tickFormat(commaFormat);
      chart.y2Axis
          .showMaxMin(false)
          .tickFormat(commaFormat);

      d3.select(id)
          .datum(data)
        .transition().duration(500)
          .call(chart);

      nv.utils.windowResize(chart.update);

      return chart;
    });
  });
};

/**
  * Save chart in its current form to a .png file.
  * button_id in the form: '#button-name'
  * chart_svg in the form: '#overall-domain svg')[0]
  * chart_name in the form: "choropleth"
  */

function pngOnClick(button_id,chart_svg,chart_name) {
    $(button_id).click(function() { //save2
    saveAsPng($(chart_svg)[0], chart_name +'.png');
  });
}

/**
 * Load an HTML file from the server and set its contents to an element.
 *
 * @param {string} filename The name of the file containing the HTML table.
 * @param {string} id The DOM ID to which to append the table.
 */
insertHTML = function(filename, id) {
  $.get(dataDirectory + filename, function(data) {
    $(id).html(data);
  });
};

/**
 * Generates a wordcloud based on a word-freq CSV generated from an R-script
 *  that updates daily. Requires D3 and d3.layout.cloud to be loaded.
 *  Filename should always be word/freq csv.
 *  TO DO: Fix Boundaries Issues - see @Jason Davies work
 */
function create_cloud(filename,id){
  d3.csv(dataDirectory + filename, function(error, data) {
          // Make sure our numbers are really numbers
          data.forEach(function(d) {
              d.text = d.word; // d.text
              d.size = d.freq; // d.size
          });

          console.log(data);
          var fill = d3.scale.category20();

          d3.layout.cloud().size([500, 300]) // TO DO: dynamic options
              .words(data)
              .padding(5)
              .rotate(0)
              .font("Impact") // default to Impact
              .fontSize(function(d) {
              return d.size;
          })
              .on("end", draw)
              .start();

          // Good , don't mess with this too much anymore
          function draw(words) {
              d3.select(id).append("svg") // body
                  .attr("width", 650)
                  .attr("height", 350)
                  .attr("class","wordcloud")
                  .append("g")
                  .attr("transform", "translate(400,0)") //150,150
                  .selectAll("text")
                  .data(words)
                  .enter().append("text")
                  .style("font-size", function(d) {
                  return d.size + "px";
              })
                  .style("font-family", "Impact")
                  .style("fill", function(d, i) {
                  return fill(i);
              })
                  .attr("text-anchor", "middle")
                  .attr("transform", function(d) {
                  return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
              })
                  .text(function(d) {
                  return d.text;
              });
          }
      });}

/*
 * Renders a sentiment Choropleth at the state level.
 * Legend labels are ridiculously arbitrary. Mansoor should be more creative. 
 * Mostly Works !
 */
function newschoroplethChart(filename,ref_id){

    var legend_labels = ["Sarah McLachlan SPCA Ad","Feeling like that dude Fortunado","George R.R. Martin has killed again.","Anyone have an iPhone charger? ", "not bad, hbu", "A No RBI Single to Left Field", "Danny Tanner", "Exit Status 0", "TURN DOWN FOR WHAT?!"] 

    var width = 960,
        height = 600,
        log = d3.scale.log(), //move to log ?
        quantize = d3.scale.quantize(),
        numberIntervals = 6;

    var rateById = d3.map();
     
    var quantize = d3.scale.quantize()
        .domain([-5, 25]) // adjust as needed? -50,150
        .range(d3.range(9).map(function(i) { return "q" + i + "-9"; }));
     
    var projection = d3.geo.albersUsa()
        .scale(1280)
        .translate([width / 2, height / 2]);
     
    var path = d3.geo.path()
        .projection(projection);
     
    var svg = d3.select(ref_id).append("svg")
        .attr("width", width)
        .attr("height", height);
     
    queue()
        .defer(d3.json, "/static/data/us.json")
        .defer(d3.csv, dataDirectory + filename, function(d) { rateById.set(d.id, +d.rate); }) //maybe allow this to change
        .await(ready);
     
    function ready(error, us) {
      svg.append("g")
          .attr("class", "states")
        .selectAll("path")
          .data(topojson.feature(us, us.objects.states).features)
        .enter().append("path")
        .attr("class", function(d) { return quantize(rateById.get(d.id)); })
        .attr("d", path);
    }

    // 'EXPERIMENTAL'
var col_r;
var text;
var updates_list = ["My tea\'s gone cold I\'m wondering why...I got out of bed at all","Just an average everyday normal day.","Not bad, hbu?", "Feeling like that one song by Pharrell, ya know with all the clapping and dangerous dancing down sidewalks", "Feeling like I wanna shotgun a beer while listening to Two Step live", "Danny Tanner", "\'98 Seattle Mariners", "code compiled successfully","TURN DOWN FOR WHAT?!"];

value_blah = 6; // for debugging

// rarely does this actually change. as expected?

// for updating status text on page, really doesn't belong here
d3.csv('/static/data/twitter_project/tmp_data_test/news-sent-single.csv', function(error, data) {
          // Make sure our numbers are really numbers
          data.forEach(function(d) {
              z_sentiment = +d.sentiment; // d.text
          });

if (z_sentiment < 0) { // negative, unlikely
   text = updates_list[0];
   col_r = "#A0A0A0";
    document.getElementById("myP").style.cssText="color:"+ col_r + ";"
    document.getElementById("myP").innerHTML = text;
} 
else if 
  (z_sentiment >= 0 && z_sentiment >= 5) { // not so great, upper-bound on the verge of average
  col_r ="#660099"
  text = updates_list[1];
  document.getElementById("myP").style.cssText="color:"+ col_r + ";"
  document.getElementById("myP").innerHTML = text;
} 
else if (z_sentiment > 5 && z_sentiment <= 9){ // lower neutral
    text = updates_list[2];
    col_r ="#6666FF"
    document.getElementById("myP").style.cssText="color:"+ col_r + ";"
    document.getElementById("myP").innerHTML = text;
}
else  if (z_sentiment > 9 && z_sentiment <= 15){ // perfectly neutral
   text = updates_list[3];
   col_r ="#FF6600"
       document.getElementById("myP").style.cssText="color:"+ col_r + ";"
    document.getElementById("myP").innerHTML = text;
} 
else if (z_sentiment > 15) { // good
  text = updates_list[4];
  col_r ="#FFFF00"
  document.getElementById("myP").style.cssText="color:"+ col_r + ";"
  document.getElementById("myP").innerHTML = text;
}
})
// END FUNCTION THAT DOESN'T BELONG

      var legendData = quantize.range()
      .map(quantize.invertExtent)
      .map(function(d) { return d.map(log.invert); })
      .map(function(d) { return d.map(function(i) { return d3.round(i, 0) }); })
      .map(function(d, i) {
        if (i === (numberIntervals - 1)) {
          return "[" + d[0] + " - " + d[1] + "]"
        } else {
          return "[" + d[0] + " - " + d[1] + ")"
        }
      });

      var legendWidth = 20,
          legendHeight = 20;

      var legend = svg.selectAll("g.legend")
      .data(legendData)
      .enter().append("g")
      .attr("class", "legend");

      var ls_w = 20, ls_h = 20;

      legend.append("rect")
      .attr("x", width - 75)
      .attr("y", function(d, i) { return height - i * legendHeight - 10 * legendHeight;})
      .attr("width", ls_w)
      .attr("height", ls_h)
      .attr("class", function(d, i) { return quantize.range()[i]; })
      .style("fill", function(d, i) { return quantize(d); })
      .style("opacity", 0.8);

      legend.append("text")
      .attr("class", "title")
      .attr("x", width - 50)
      .attr("y", 225)
      .text("Corresponding Feelings");

      legend.append("text")
      .attr("x", width - 45)
      .attr("y", function(d, i) { return height - i * legendHeight - 9.25 * legendHeight;})
      .text(function(d, i){ return legend_labels[i]; });

    d3.select(ref_id).style("height", height + "px");
}

/********** EXPERIMENTAL **************/

/*
// make into function
var variables = {
  "Region": {
   property: "rrate"
  },
  "SubRegion": {
   property:"srate"
  },
  "Congressional Districts": {
   property:"crate"
  }
};

d3.select("#variableSelect").on("change",e);
e();

function e() {

   var m = d3.select("#variableSelect").property("value");

   d3.csv("/static/data/twitter_project/tmp_data_test/news_sent_edit.csv", function(error, data) {
      data.forEach(function(d) {
          d.id = d.id;
          d.rate = "+d." + m;
      });

  rateById.set(d.id, +d.rate);

   var svg = d3.select("#choro-alt").transition();
      svg.append("g")
        .attr("class", "states")
        .selectAll("path")
        //.data(topojson.feature(us, us.objects.states).features)
        .enter().append("path")
        .attr("class", function(d) { return quantize(rateById.get(d.id)); })
        .attr("d", path);


   });}

*/

/******* end expiremental ***************/

// note the open paren on 121, that stays open until we no longer need to be within scope of the data

function(error, data) {
  var colorScale = d3.scale.quantile()
      .domain([ 0 ,5])
      .range(colors);
  
  var svg = d3.select(ref_id).append("svg") // note we append our SVG to a div-ref(id), it's static here, we'll need to create a function that allows us to input our own div so we can reuse the function
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(100,50)");
      //.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
      
  var rowSortOrder=false;
  var colSortOrder=false;
  var rowLabels = svg.append("g")
      .selectAll(".rowLabelg")
      .data(rowLabel) // sets the data for rowLabels, you could alternatively copy the array rowLabel into the parens
      .enter()
      .append("text")
      .text(function (d) { return d; })
      .attr("x", 0)
      .attr("y", function (d, i) { return hcrow.indexOf(i+1) * cellSize; })
      .style("text-anchor", "end")
      .attr("transform", "translate(-6," + cellSize / 1.5 + ")")
      .attr("class", function (d,i) { return "rowLabel mono r"+i;} ) 
      .on("mouseover", function(d) {d3.select(this).classed("text-hover",true);})
      .on("mouseout" , function(d) {d3.select(this).classed("text-hover",false);})
      .on("click", function(d,i) {rowSortOrder=!rowSortOrder; sortbylabel("r",i,rowSortOrder);d3.select("#order").property("selectedIndex", 4).node().focus();;})
      ;

  var colLabels = svg.append("g")
      .selectAll(".colLabelg")
      .data(colLabel)
      .enter()
      .append("text")
      .text(function (d) { return d; })
      .attr("x", 0)
      .attr("y", function (d, i) { return hccol.indexOf(i+1) * cellSize; })
      .style("text-anchor", "left")
      .attr("transform", "translate("+cellSize/2 + ",-6) rotate (-90)")
      .attr("class",  function (d,i) { return "colLabel mono c"+i;} )
      .on("mouseover", function(d) {d3.select(this).classed("text-hover",true);})
      .on("mouseout" , function(d) {d3.select(this).classed("text-hover",false);})
      .on("click", function(d,i) {colSortOrder=!colSortOrder;  sortbylabel("c",i,colSortOrder);d3.select("#order").property("selectedIndex", 4).node().focus();;})
      ;

  var heatMap = svg.append("g").attr("class","g3")
        .selectAll(".cellg")
        .data(data,function(d){return d.row+":"+d.col;})
        .enter()
        .append("rect")
        .attr("x", function(d) { return (d.col - 1) * cellSize; })
        .attr("y", function(d) { return (d.row - 1) * cellSize; })
        //.attr("x", function(d) { return hccol.indexOf(d.col) * cellSize; })
        //.attr("y", function(d) { return hcrow.indexOf(d.row) * cellSize; })
        .attr("class", function(d){return "cell cell-border cr"+(d.row-1)+" cc"+(d.col-1);})
        .attr("width", cellSize)
        .attr("height", cellSize)
        .style("fill", function(d) { return colorScale(d.value); })
        /* .on("click", function(d) {
               var rowtext=d3.select(".r"+(d.row-1));
               if(rowtext.classed("text-selected")==false){
                   rowtext.classed("text-selected",true);
               }else{
                   rowtext.classed("text-selected",false);
               }
        })*/
        .on("mouseover", function(d){
               //highlight text
               d3.select(this).classed("cell-hover",true);
               d3.selectAll(".rowLabel").classed("text-highlight",function(r,ri){ return ri==(d.row-1);});
               d3.selectAll(".colLabel").classed("text-highlight",function(c,ci){ return ci==(d.col-1);});
        
               //Update the tooltip position and value
               d3.select("#tooltip")
                 .style("left", (d3.event.pageX+10) + "px")
                 .style("top", (d3.event.pageY-10) + "px")
                 .select("#value")
                 .text("lables:"+rowLabel[d.row-1]+","+colLabel[d.col-1]+"\ndata:"+d.value+"\nrow-col-idx:"+d.col+","+d.row+"\ncell-xy "+this.x.baseVal.value+", "+this.y.baseVal.value);  
               //Show the tooltip
               d3.select("#tooltip").classed("hidden", false);
        })
        .on("mouseout", function(){
               d3.select(this).classed("cell-hover",false);
               d3.selectAll(".rowLabel").classed("text-highlight",false);
               d3.selectAll(".colLabel").classed("text-highlight",false);
               d3.select("#tooltip").classed("hidden", true);
        })
        ;

  var legend = svg.selectAll(".legend")
      .data([0,.5,1,1.5,2,2.5,3,3.5,4,4.5,5,5.5,100]) //hard-coded legend data in here which is usually considered cheating, but for us, it actually makes sense to just hardcode in the similarity value labels from 0 to 1 
      .enter().append("g")
      .attr("class", "legend");
 
  legend.append("rect")
    .attr("x", function(d, i) { return legendElementWidth * i; })
    .attr("y", height+(cellSize*2))
    .attr("width", legendElementWidth)
    .attr("height", cellSize)
    .style("fill", function(d, i) { return colors[i]; });
 
  legend.append("text")
    .attr("class", "mono")
    .text(function(d) { return d; })
    .attr("width", legendElementWidth)
    .attr("x", function(d, i) { return legendElementWidth * i; })
    .attr("y", height + (cellSize*4));
  legend.attr("transform", "translate(150,75)");

// Change ordering of cells

  function sortbylabel(rORc,i,sortOrder){
       var t = svg.transition().duration(3000);
       var log2r=[];
       var sorted; // sorted is zero-based index
       d3.selectAll(".c"+rORc+i) 
         .filter(function(ce){
            log2r.push(ce.value);
          })
       ;
       if(rORc=="r"){ // sort log2ratio of a gene
         sorted=d3.range(col_number).sort(function(a,b){ if(sortOrder){ return log2r[b]-log2r[a];}else{ return log2r[a]-log2r[b];}});
         t.selectAll(".cell")
           .attr("x", function(d) { return sorted.indexOf(d.col-1) * cellSize; })
           ;
         t.selectAll(".colLabel")
          .attr("y", function (d, i) { return sorted.indexOf(i) * cellSize; })
         ;
       }else{ // sort log2ratio of a contrast
         sorted=d3.range(row_number).sort(function(a,b){if(sortOrder){ return log2r[b]-log2r[a];}else{ return log2r[a]-log2r[b];}});
         t.selectAll(".cell")
           .attr("y", function(d) { return sorted.indexOf(d.row-1) * cellSize; })
           ;
         t.selectAll(".rowLabel")
          .attr("y", function (d, i) { return sorted.indexOf(i) * cellSize; })
         ;
       }
  }

  d3.select("#order").on("change",function(){ // on line 175 we created an ID for selecting different display options, this says when the value changes, run this function. I actually don't like how he does this here. I'll modify these later on. 
    order(this.value);
  });
  
  // everything below is tooltip, applying changes based on form selection
  function order(value){
   if(value=="hclust"){ //hclust
    var t = svg.transition().duration(3000);
    t.selectAll(".cell")
      .attr("x", function(d) { return hccol.indexOf(d.col) * cellSize; })
      .attr("y", function(d) { return hcrow.indexOf(d.row) * cellSize; })
      ;

    t.selectAll(".rowLabel")
      .attr("y", function (d, i) { return hcrow.indexOf(i+1) * cellSize; })
      ;

    t.selectAll(".colLabel")
      .attr("y", function (d, i) { return hccol.indexOf(i+1) * cellSize; })
      ;

   }else if (value=="probecontrast"){ //probecontrast
    var t = svg.transition().duration(3000);
    t.selectAll(".cell")
      .attr("x", function(d) { return (d.col - 1) * cellSize; })
      .attr("y", function(d) { return (d.row - 1) * cellSize; })
      ;

    t.selectAll(".rowLabel")
      .attr("y", function (d, i) { return i * cellSize; })
      ;

    t.selectAll(".colLabel")
      .attr("y", function (d, i) { return i * cellSize; })
      ;

   }else if (value=="probe"){
    var t = svg.transition().duration(3000);
    t.selectAll(".cell")
      .attr("y", function(d) { return (d.row - 1) * cellSize; })
      ;

    t.selectAll(".rowLabel")
      .attr("y", function (d, i) { return i * cellSize; })
      ;
   }else if (value=="contrast"){
    var t = svg.transition().duration(3000);
    t.selectAll(".cell")
      .attr("x", function(d) { return (d.col - 1) * cellSize; })
      ;
    t.selectAll(".colLabel")
      .attr("y", function (d, i) { return i * cellSize; })
      ;
   }
  }
  // 
  var sa=d3.select(".g3")
      .on("mousedown", function() {
          if( !d3.event.altKey) {
             d3.selectAll(".cell-selected").classed("cell-selected",false);
             d3.selectAll(".rowLabel").classed("text-selected",false);
             d3.selectAll(".colLabel").classed("text-selected",false);
          }
         var p = d3.mouse(this);
         sa.append("rect")
         .attr({
             rx      : 0,
             ry      : 0,
             class   : "selection",
             x       : p[0],
             y       : p[1],
             width   : 1,
             height  : 1
         })
      })
      .on("mousemove", function() {
         var s = sa.select("rect.selection");
      
         if(!s.empty()) {
             var p = d3.mouse(this),
                 d = {
                     x       : parseInt(s.attr("x"), 10),
                     y       : parseInt(s.attr("y"), 10),
                     width   : parseInt(s.attr("width"), 10),
                     height  : parseInt(s.attr("height"), 10)
                 },
                 move = {
                     x : p[0] - d.x,
                     y : p[1] - d.y
                 }
             ;
      
             if(move.x < 1 || (move.x*2<d.width)) {
                 d.x = p[0];
                 d.width -= move.x;
             } else {
                 d.width = move.x;       
             }
      
             if(move.y < 1 || (move.y*2<d.height)) {
                 d.y = p[1];
                 d.height -= move.y;
             } else {
                 d.height = move.y;       
             }
             s.attr(d);
      
                 // deselect all temporary selected state objects
             d3.selectAll('.cell-selection.cell-selected').classed("cell-selected", false);
             d3.selectAll(".text-selection.text-selected").classed("text-selected",false);

             d3.selectAll('.cell').filter(function(cell_d, i) {
                 if(
                     !d3.select(this).classed("cell-selected") && 
                         // inner circle inside selection frame
                     (this.x.baseVal.value)+cellSize >= d.x && (this.x.baseVal.value)<=d.x+d.width && 
                     (this.y.baseVal.value)+cellSize >= d.y && (this.y.baseVal.value)<=d.y+d.height
                 ) {
      
                     d3.select(this)
                     .classed("cell-selection", true)
                     .classed("cell-selected", true);

                     d3.select(".r"+(cell_d.row-1))
                     .classed("text-selection",true)
                     .classed("text-selected",true);

                     d3.select(".c"+(cell_d.col-1))
                     .classed("text-selection",true)
                     .classed("text-selected",true);
                 }
             });
         }
      })
      .on("mouseup", function() {
            // remove selection frame
         sa.selectAll("rect.selection").remove();
      
             // remove temporary selection marker class
         d3.selectAll('.cell-selection').classed("cell-selection", false);
         d3.selectAll(".text-selection").classed("text-selection",false);
      })
      .on("mouseout", function() {
         if(d3.event.relatedTarget.tagName=='html') {
                 // remove selection frame
             sa.selectAll("rect.selection").remove();
                 // remove temporary selection marker class
             d3.selectAll('.cell-selection').classed("cell-selection", false);
             d3.selectAll(".rowLabel").classed("text-selected",false);
             d3.selectAll(".colLabel").classed("text-selected",false);
         }
      })
      ;
});
}
/******** Adjacency Matrix *********/
function adj_matrx(filename,id){

  var margin = {top: 80, right: 50, bottom: 10, left: 80},
  width = 720,
  height = 720;

  var x = d3.scale.ordinal().rangeBands([0, width]),
  //z = d3.scale.linear().domain([0, 100]).clamp(true),
  z = d3.scale.linear().domain([0, 25]).clamp(true),
  c = d3.scale.category10().domain(d3.range(10));

  var svg = d3.select(id).append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .style("margin-left", -margin.left + "px")
  .append("g")
  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  d3.json(dataDirectory + filename, function(d) {
    var matrix = [],
    nodes = d.nodes,
    n = nodes.length,
    linksm = d.links,
    n2 = linksm.length,
    matrix2 = [];

  // Compute index per node.
  nodes.forEach(function(node, i) {
    node.index = i;
    node.count = 0;
    matrix[i] = d3.range(n).map(function(j) { return {x: j, y: i, z: 0}; });
  });
  linksm.forEach(function(link,i) {
    matrix2[i] = [];
    matrix2[link.source][link.target] = link.value;
  });

  // Convert links to matrix; count character occurrences.
  d.links.forEach(function(link) {
    matrix[link.source][link.target].z += link.value;
    matrix[link.target][link.source].z += link.value;
    matrix[link.source][link.source].z += link.value;
    matrix[link.target][link.target].z += link.value;
    // matrix2[link.source][link.target].z = link.value;
    nodes[link.source].count += link.value;
    nodes[link.target].count += link.value;

  });

  // Precompute the orders.
  var orders = {
    name: d3.range(n).sort(function(a, b) { return d3.ascending(nodes[a].name, nodes[b].name); }),
    count: d3.range(n).sort(function(a, b) { return nodes[b].count - nodes[a].count; }),
    group: d3.range(n).sort(function(a, b) { return nodes[b].group - nodes[a].group; })
  };

  // The default sort order.
  x.domain(orders.name);

  svg.append("rect")
  .attr("class", "background")
  .attr("width", width)
  .attr("height", height)
  .attr("fill","#ffffff");

  var row = svg.selectAll(".row")
  .data(matrix)
  .enter().append("g")
  .attr("class", "row")
  .attr("transform", function(d, i) { return "translate(0," + x(i) + ")"; })
  .each(row);

  row.append("line")
  .attr("x2", width);

  row.append("text")
  .attr("x", -6)
  .attr("y", x.rangeBand() / 2)
  .attr("dy", ".32em")
  .attr("text-anchor", "end")
  .text(function(d, i) { return nodes[i].name; });

  var column = svg.selectAll(".column")
  .data(matrix)
  .enter().append("g")
  .attr("class", "column")
  .attr("transform", function(d, i) { return "translate(" + x(i) + ")rotate(-90)"; });

  column.append("line")
  .attr("x1", -width);

  column.append("text")
  .attr("x", 6)
  .attr("y", x.rangeBand() / 2)
  .attr("dy", ".32em")
  .attr("text-anchor", "start")
  .text(function(d, i) { return nodes[i].name; });

  function row(row) {
    var cell = d3.select(this).selectAll(".cell")
    .data(row.filter(function(d) { return d.z; }))
    .enter().append("rect")
    .attr("class", "cell")
    .attr("x", function(d) { return x(d.x); })
    .attr("width", x.rangeBand())
    .attr("height", x.rangeBand())
    .style("fill-opacity", function(d) { return z(d.z); })
    .style("fill", function(d) { return nodes[d.x].group == nodes[d.y].group ? c(nodes[d.x].group + 7) : null; })
    .append("svg:title")
    .text(function(d) { return  nodes[d.x].name + ', ' + nodes[d.y].name + ': ' + matrix2[d.x][d.y].toFixed(2).toString() + '% similar' ;});
  }

  function mouseover(p) {
    d3.selectAll(".row text").classed("active", function(d, i) { return i == p.y; });
    d3.selectAll(".column text").classed("active", function(d, i) { return i == p.x; });
  }

  function mouseout() {
    d3.selectAll("text").classed("active", false);
  }

  d3.select("#order").on("change", function() {
    clearTimeout(timeout);
    order(this.value);
  });

  function order(value) {
    x.domain(orders[value]);

    var t = svg.transition().duration(2500);

    t.selectAll(".row")
    .delay(function(d, i) { return x(i) * 4; })
    .attr("transform", function(d, i) { return "translate(0," + x(i) + ")"; })
    .selectAll(".cell")
    .delay(function(d) { return x(d.x) * 4; })
    .attr("x", function(d) { return x(d.x); });

    t.selectAll(".column")
    .delay(function(d, i) { return x(i) * 4; })
    .attr("transform", function(d, i) { return "translate(" + x(i) + ")rotate(-90)"; });
  }

  var timeout = setTimeout(function() {
    order("group");
    d3.select("#order").property("selectedIndex", 2).node();
  }, 5000);
});
}

/********** Markov Movie Synopses Generator **************/
/** JS adapted from 'roth blog generator' rpetchler '13
 * Randomly select a key from an object whose values are probabilities of
 * selection.
 *
 * @param {Object} d an object whose values are probabilities (i.e., floats
 *   that sum to one).
 * @return {Object} a key randomly chosen according to the probabilities.
 */
weightedRandomChoice = function(d) {
  var sum = 0,
      r = Math.random();

  for (var i in d) {
    sum += d[i];
    if (r <= sum) return i;
  }
}

generateSentence = function() {
  var key,
      text = weightedRandomChoice(heads).split(" ");

  while (true) {
    key = text.slice(-2).join(" ");
    text.push(weightedRandomChoice(d[key]));

    // End if the sentence is long and the trailing bigram is a sentence tail.
    if (text.length >= 10 && tails.indexOf(key) > -1) break;

    // Truncate long sentences to prevent infinite loops.
    if (text.length >= 100) break;
  }

  return $.trim(text.join(" ")) + ".";
}

/**
 * Update the block quote with Markov-generated text.
 *
 * @return {null} Updates the HTML of the block quote.
 */
function markov_gen(filename,id){
generateText = function() {
  var paragraphs = [],
      sentences;

  for (var i = 1; i <= 1; i++) {  // Paragraph count
    sentences = [];
    for (var j = 1; j <= 2; j++) {  // Sentence count
      sentences.push(generateSentence());
    }
    paragraphs.push(sentences.join(" "));
  }

  return paragraphs.join("</p><p>");
}

update = function() {
  $("#markov_results").html(generateText());
}

$.getJSON(dataDirectory + filename, function(data) { //json file
  heads = data.heads;
  tails = data.tails;
  d = data.d;
  update();
  $("#generate").prop("disabled", false);
});

$("#generate").click(update);
}


/********** Tab Handlers for Twitter Streams *************/
var healthCurrentAgg = "hour";

$(".btn-hc-sent").click(function() {
  healthRequestAgg = $(this).text().toLowerCase();
  if (healthRequestAgg !== healthCurrentAgg) {
    switch (healthRequestAgg) {
      case "hour":
        document.getElementById("hc-sent").innerHTML = "<svg></svg>";
        lineWithFocusChart("hc-sent-hour.json", "#hc-sent svg", datetimeFormat);
        break;
      case "day":
        document.getElementById("hc-sent").innerHTML = "<svg></svg>";
        lineWithFocusChart("hc-sent-day.json", "#hc-sent svg", dateFormat);
        break;
    }
    healthCurrentAgg = healthRequestAgg;
  }
});

/* Call Functions (note removal of tab handlers) ********/

  newschoroplethChart("news_sent_total.csv","#news-choro"); //news_sent_edit.csv
  create_cloud2("word-freq-news-all.csv","#word-cloud-news svg");
  heatMapChart("data_heatmap.csv","#heatmap");
  lineWithFocusChart("hc-sent-hour.json", "#hc-sent svg", datetimeFormat);
  insertHTML("gtrends.html", "#gtrends");
  getCurDate();
  adj_matrx("ajmatrix.json","#adj_matrx svg");
  markov_gen("markov.json","#markov_results");
  movieLine("daily_movies.csv","#movie-line");


