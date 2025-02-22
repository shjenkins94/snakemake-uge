#!/usr/bin/env bash
# properties = {properties}

# Create directory to hold exit status files
mkdir -p "{{cookiecutter.default_cluster_statdir}}"

# print cluster job id
echo "Running cluster job $JOB_ID"
echo "-----------------------------"

# run the job command
( {exec_job} )
echo $? > {{cookiecutter.default_cluster_statdir}}/${{"{{JOB_ID}}"}}.exit #Store exit status in a file

# print exit status
echo "-----------------------------"
printf "Exit Status: " | cat - {{cookiecutter.default_cluster_statdir}}/${{"{{JOB_ID}}"}}.exit
echo "-----------------------------"

# exit with captured exit status
exit $(<{{cookiecutter.default_cluster_statdir}}/${{"{{JOB_ID}}"}}.exit)
