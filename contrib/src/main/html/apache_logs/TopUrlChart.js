/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawTopUrlTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        if (pts.length > topUrlTable.getNumberOfRows())
        {
          var numRows = pts.length - topUrlTable.getNumberOfRows();
          topUrlTable.addRows(numRows);      
        } 
        for(var i=0; i <  pts.length; i++) 
        {
          topUrlTable.setCell(0, i, pts[i]);
          delete pts[i];
        }
        delete pts;
        delete data;
        topUrlTableChart.draw(topUrlTable, {showRowNumber: true});
        //document.getElementById('top_url_div').innerHTML = data;
      }
    }
    connect.open('GET',  "/TopUrlData.php", true);
    connect.send(null);
  } catch(e) {
  }
}