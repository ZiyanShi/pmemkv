#!/bin/bash

set -x

function FROM() {
	echo $@
}

function MAINTAINER {
	echo $@
}

function ENV {
	export ${1}=${2}
}

function ARG() {
	#TODO: Add env variables to .bashrc to set it permanent
	 ENV $@
	env
}

function RUN() {
	$@
}

function COPY() {
	#TODO: Make copying more generic
	echo "Not copying any files. scripts would be run from ${PWD}"
}

function USER(){
	echo $@
}

dockerfiles=${@}
for dockerfile in ${dockerfiles}; do
	source ${1}
done

