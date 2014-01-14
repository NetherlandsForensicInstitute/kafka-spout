[![Build Status (development branch)](https://travis-ci.org/HolmesNL/kafka-spout.png?branch=develop)](https://travis-ci.org/HolmesNL/kafka-spout)
[![Coverage Status (development branch)](https://coveralls.io/repos/HolmesNL/kafka-spout/badge.png?branch=develop)](https://coveralls.io/r/HolmesNL/kafka-spout?branch=develop)

Kafka spout
===========
Storm spout implementation reading messages from a kafka topic and emits these as single field tuples into a storm topology.
Documentation is available on [the wiki](https://github.com/HolmesNL/kafka-spout/wiki).

Development
-----------
This implementation was created by the Netherlands Forensic Institute and is still under development.
The project was tested with kafka_2.10 version **0.8.0beta1** and storm version **0.9.0rc3**.
Contributions are welcome, please read [the contribution guidelines](./CONTRIBUTING.md).

Java versions
-------------
The code is compatible with both Java 6 and Java 7.

License
-------
This work is licensed under the Apache License, Version 2.0.
See [LICENSE](./LICENSE) for details.
