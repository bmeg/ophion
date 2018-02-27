(defproject ophion "0.0.13"
  :description "graph queries as data"
  :url "http://github.com/bmeg/ophion"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [aleph "0.4.3"]
                 [polaris "0.0.19"]
                 [cheshire "5.7.1"]
                 [com.taoensso/timbre "4.8.0"]
                 ;; [org.immutant/web "2.1.7"]
                 [http-kit "2.2.0"]
                 [protograph "0.0.19"]
                 [clojurewerkz/elastisch "2.2.1" :exclusions [commons-codec]]
                 [com.novemberain/monger "3.1.0"]
                 [;;org.janusgraph/janusgraph-core "0.2.0-20170924.171957-19"
                  org.janusgraph/janusgraph-core "0.2.0-20171010.201123-25"
                  ;; org.janusgraph/janusgraph-core "0.2.0-SNAPSHOT"
                  :exclusions [org.slf4j/slf4j-api
                               org.yaml/snakeyaml]]
                 [;;org.janusgraph/janusgraph-cassandra "0.2.0-20170924.172029-17"

                  org.janusgraph/janusgraph-cassandra "0.2.0-20171010.201157-23"
                  ;; org.janusgraph/janusgraph-cassandra "0.2.0-SNAPSHOT"
                  :exclusions [org.xerial.snappy/snappy-java
                               org.slf4j/slf4j-api
                               org.yaml/snakeyaml]]]


                 ;; [org.janusgraph/janusgraph-es "0.2.0-20170924.171957-19"
                 ;;  :exclusions [org.slf4j/slf4j-api
                 ;;               org.yaml/snakeyaml]]

  :repositories [["sonatype snapshots"
                  "https://oss.sonatype.org/content/repositories/snapshots"]
                 ["sonatype releases"
                  "https://oss.sonatype.org/content/repositories/releases"]]
  :jvm-opts ["-Xmx4g" "-Xms4g"]
  :main ophion.server)
