(defproject ophion "0.0.2"
  :description "graph queries as data"
  :url "http://github.com/bmeg/ophion"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [aleph "0.4.3"]
                 [polaris "0.0.17"]
                 [cheshire "5.7.1"]
                 [com.taoensso/timbre "4.8.0"]
                 ;; [org.immutant/web "2.1.6"]
                 [protograph "0.0.4-SNAPSHOT"
                  :exclusions [org.scala-lang.modules/scala-xml_2.11
                               org.slf4j/slf4j-log4j12]]
                 [clojurewerkz/elastisch "2.2.1"]
                 ;; [io.bmeg/protograph_2.11 "0.0.1-SNAPSHOT"]
                 ;; [clojurewerkz/propertied "1.2.0"]
                 ;; [org.apache.kafka/kafka_2.10 "0.10.0.1" :scope "test"
                 ;;  :exclusions [io.netty/netty
                 ;;               log4j
                 ;;               org.slf4j/slf4j-api
                 ;;               org.slf4j/slf4j-log4j12]]
                 ;; [org.clojars.ghaskins/protobuf "3.0.2-2"]
                 [org.apache.tinkerpop/tinkergraph-gremlin "3.2.3"]
                 [org.janusgraph/janusgraph-core "0.2.0-SNAPSHOT"
                  :exclusions [org.slf4j/slf4j-api
                               org.yaml/snakeyaml]]
                 [org.janusgraph/janusgraph-cassandra "0.2.0-SNAPSHOT"
                  :exclusions [org.xerial.snappy/snappy-java
                               org.slf4j/slf4j-api
                               org.yaml/snakeyaml]]
                 [org.janusgraph/janusgraph-es "0.2.0-SNAPSHOT"
                  :exclusions [org.slf4j/slf4j-api
                               org.yaml/snakeyaml]]]
  :repositories [["sonatype snapshots"
                  "https://oss.sonatype.org/content/repositories/snapshots"]
                 ["sonatype releases"
                  "https://oss.sonatype.org/content/repositories/releases"]]
  :jvm-opts ["-Xmx12g" "-Xms12g"]
  :main ophion.aleph)
