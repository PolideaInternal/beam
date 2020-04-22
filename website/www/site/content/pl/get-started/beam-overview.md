---
title: "Beam Overview | PL"
aliases:
  - /use/beam-overview/
  - /docs/use/beam-overview/
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

# Przegląd wiązki Apache

Apache Beam to zunifikowany model o otwartym kodzie źródłowym do definiowania zarówno przetwarzania wsadowego, jak i strumieniowego przetwarzania danych równolegle przetwarzanych. Korzystając z jednego z otwartych źródeł Beam SDK, budujesz program, który definiuje potok. Potok jest następnie wykonywany przez jeden z obsługiwanych przez Beam **rozproszonych procesorów przetwarzania**, które obejmują [Apache Apex](http://apex.apache.org), [Apache Flink](http://flink.apache.org),[Apache Spark](http://spark.apache.org) i [Google Cloud Dataflow](https://cloud.google.com/dataflow).

Wiązka jest szczególnie przydatna w przypadku zadań przetwarzania danych [kłopotliwie równoległych](http://en.wikipedia.org/wiki/Embarassingly_parallel), w których problem można rozłożyć na wiele mniejszych pakietów danych, które można przetwarzać niezależnie i równolegle. Możesz także użyć Beam do zadań wyodrębniania, przekształcania i ładowania (ETL) oraz czystej integracji danych. Te zadania są przydatne do przenoszenia danych między różnymi nośnikami pamięci i źródłami danych, przekształcania danych w bardziej pożądany format lub ładowania danych do nowego systemu.

## Apache Beam SDKs

Zestawy SDK wiązki zapewniają ujednolicony model programowania, który może reprezentować i przekształcać zestawy danych o dowolnej wielkości, niezależnie od tego, czy dane wejściowe są skończonym zestawem danych ze źródła danych wsadowych, czy nieskończonym zestawem danych ze źródła danych strumieniowych. Zestawy SDK wiązki używają tych samych klas do reprezentowania zarówno danych ograniczonych, jak i niezwiązanych oraz tych samych transformacji, aby operować na tych danych. Korzystasz z wybranego zestawu SDK Beam, aby zbudować program, który definiuje potok przetwarzania danych.

Obecnie Beam obsługuje następujące zestawy SDK specyficzne dla języka:

- Java ![Java logo](/images/logos/sdks/java.png)
- Python ![Logo Python](/images/logos/sdks/python.png)
- Idź <img src = "/images/logos/sdks/go.png" height="45px" alt ="Idź logo">

Interfejs Scala <img src ="/images/logos/sdks/scala.png" height="45px" alt="Scala logo"> jest również dostępny jako [Scio](https://github.com/spotify/scio).

## Apache Beam Pipeline Runners

Beam Pipeline Runners tłumaczą potok przetwarzania danych zdefiniowany w programie Beam na API kompatybilny z wybranym zapleczem przetwarzania rozproszonego. Kiedy uruchamiasz program Beam, musisz określić [odpowiedni runner](/dokumentacja/runners/capability-matrix) dla zaplecza, w którym chcesz uruchomić potok.

Obecnie Beam obsługuje moduły Runner współpracujące z następującymi zapleczami przetwarzania rozproszonego:

- Apache Apex ![Logo Apache Apex](/images/logos/runners/apex.png)
- Apache Flink ![Logo Apache Flink](/images/logos/runners/flink.png)
- Apache Gearpump (inkubacja) ![LogoApacheGearpump](/images/logos/runners/gearpump.png)
- Apache Samza <img src="/images/logos/runners/samza.png" height="20px" alt="Logo Apache Samza">
- Apache Spark ![Logo Apache Spark](/images/logos/runners/spark.png)
- Google Cloud Dataflow ![Logo Google Cloud Dataflow](/images/logos/runners/dataflow.png)
- Hazelcast Jet ![Hazelcast Jet logo](/images/logos/runners/jet.png)

**Uwaga:** Zawsze możesz uruchomić potok lokalnie w celu testowania i debugowania.

## Zaczynaj

Rozpocznij korzystanie z Beam do zadań przetwarzania danych.

1. [Wypróbuj Apache Beam](/get-Start/try-apache-beam) w interaktywnym środowisku online.

1. Postępuj zgodnie ze wskazówkami Szybki start dla [Java SDK](/get-start/quickstart-java), [Python SDK](/get-start/quickstart-py) lub [Go SDK](/get-start/quickstart-go).

1. Zobacz [WordCount Examples Walkthrough](/get-start/wordcount-example), aby zapoznać się z przykładami wprowadzającymi różne funkcje zestawów SDK.

1. Wybierz się na wycieczkę we własnym tempie przez nasze [Materiały szkoleniowe](/documentation/resources/learning-resources).

1. Zanurz się w sekcji [Dokumentacja](/documentation/), aby uzyskać szczegółowe koncepcje i materiały referencyjne dla modelu wiązki, zestawów SDK i prowadnic.

## Przyczynić się

Beam to projekt <a href="http://www.apache.org" target="_blank"> Apache Software Foundation </a> dostępny na licencji Apache v2. Beam jest społecznością typu open source, a wkłady są bardzo mile widziane! Jeśli chcesz przyczynić się, zobacz sekcję [Przekaż](/contribute/).