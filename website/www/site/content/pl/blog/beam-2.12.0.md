---
title:  "Apache Beam 2.12.0"
date:   2019-04-25 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/04/25/beam-2.12.0.html
authors:
        - apilloud

---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Z przyjemnością prezentujemy nową wersję Beam 2.12.0. Ta wersja zawiera zarówno ulepszenia, jak i nową funkcjonalność.
Zobacz [stronę pobierania](/get-start/download/#2120-2019-04-25) dla tego wydania.<!--more-->
Aby uzyskać więcej informacji o zmianach w 2.12.0, sprawdź
[szczegółowe informacje o wersji](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12344944).

## Najważniejsze

### I / Os

* Dodaj obsługę niestandardowego ujścia BigQuery dla zestawu Python SDK.
* Dodaj obsługę, aby określić zapytanie w CassandraIO dla Java SDK.
* Dodaj eksperymentalne wsparcie dla transformacji międzyjęzykowych, zobacz [BEAM-6730](https://issues.apache.org/jira/browse/BEAM-6730)
* Dodaj wsparcie we Flink Runner dla dokładnie jednokrotnego zapisu w KafkaIO

### Nowe funkcje / ulepszenia

* Włącz finalizację pakietu w zestawie Python SDK dla przenośnych biegaczy.
* Dodaj obsługę wiązki Java SDK, aby scalić okna.
* Dodaj obsługę EOS Kafka Sink na urządzeniu Flink runner.
* Dodano kolejkę martwych listów do zlewu BigQuery przesyłającego strumieniowo w Pythonie.
* Włączono dodawanie obciążeń eksperymentalnych w języku Python 3.6 i 3.7. Beam 2.12 obsługuje uruchamianie potoków Dataflow w Pythonie 3.6, 3.7, jednak 3.5 pozostaje jedyną zalecaną mniejszą wersją dla programu Dataflow runner. Oprócz ogłaszanych ograniczeń 2.11, adnotacje typu wiązki nie są obecnie obsługiwane w Pythonie> = 3.6.


### Poprawki błędów

* Różne poprawki błędów i ulepszenia wydajności.

## Lista autorów

Według git shortlog, następujące osoby przyczyniły się
do wersji 2.12.0. Dziękujemy wszystkim uczestnikom!

Ahmed El.Hussaini, Ahmet Altay, Alan Myrvold, Alex Amato, Alexander Savchenko,
Alexey Romanenko, Andrew Brampton, Andrew Pilloud, Ankit Jhalaria,
Ankur Goenka, Anton Kedin, Boyuan Zhang, Brian Hulette, Chamikara Jayalath,
Charles Chen, Colm O'Heartaartaigh, Craig Chambers, Dan Duong, Daniel Mescheder,
Daniel Oliveira, David Moravek, David Rieber, David Yan, Eric Roshan-Eisner,
Etienne Chauchot, Gleb Kanterov, Heejong Lee, Ho Tien Vu, Ismaël Mejía,
Jan Lukavský, Jean-Baptiste Onofré, Jeff Klukas, Juta, Kasia Kucharczyk,
Kengo Seki, Kenneth Jung, Kenneth Knowles, kevin, Kyle Weaver, Kyle Winkelman,
Łukasz Gajowy, Mark Liu, Mathieu Blanchard, Max Charas, Maximilian Michels,
Melissa Pashniak, Michael Luckey, Michal Walenia, Mike Kaplinskiy,
Michaił Gryzykhin, Niel Markwick, Pablo Estrada, Radosław Stankiewicz,
Reuven Lax, Robbe Sneyders, Robert Bradshaw, Robert Burke, Rui Wang,
Ruoyun Huang, Ryan Williams, Slava Chernyak, Shahar Frank, Sunil Pedapudi,
Thomas Weise, Tim Robertson, Tanay Tummalapalli, Udi Meiri,
Valentyn Tymofieiev, Xinyu Liu, Yifan Zou, Yueyang Qiu