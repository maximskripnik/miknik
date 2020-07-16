# Miknik
----
⚠️ Work in progress 🚧
[![Build Status](https://travis-ci.com/maximskripnik/miknik.svg?token=dYQ8y9WVPQU8KZMpENtE&branch=develop)](https://travis-ci.com/maximskripnik/miknik)
[![Coverage Status](https://coveralls.io/repos/github/maximskripnik/miknik/badge.svg?branch=develop&t=F05zrE)](https://coveralls.io/github/maximskripnik/miknik?branch=develop)

> Miknik is a temporary name which will be changed soon

Miknik is a Mesos Framework that manages cluster capacity management automatically based on the workload.
It is useful for use cases where it is hard or not practically possible to predict the workload in form of
batch jobs to be scheduled for run in the Mesos cluster. Miknik will scale up your Mesos cluster by renting
the resources from the cloud provider (Digital Ocean is currently supported, more planned) if number of
pending jobs will exceed the configured value. It will also give the rented resources back to the cloud
provider (i.e. scale cluster down) if newly rented resources keep being unused for a while.

If you happen to know Russian, check out this 'white paper' (bachelor thesis) for more details:
http://vital.lib.tsu.ru/vital/access/manager/Repository/vital:11294

Currently **Miknik is not production ready and should be considered a POC project**. However, today it already
lets you run jobs in form of docker containers with custom resource requirements that consist of
CPU, memory and disk capacity.

For usage, see HTTP API specification: <http://maximskripnik.com:5161/>
