#!/bin/bash

datadirectory=" "
rundirectory=" "

cmd1="python"$rundirectory"daily_downloader.py" > logs/daily.out
cmd2="python"$rundirectory"topicmodeling.py"
cmd3="python"$rundirectory"news_visualizations.py"
cmd4="R CMD BATCH"$rundirectory"topic_graph.R"
cmd5="python"$rundirectory"download_alert.py"

$cmd1 > daily.out
if [ $? -eq 0 ]
then
    $cmd2 && $cmd3 && $cmd4 
else
    $cmd5
fi
