(defproject ophion "0.0.1"
  :description "graph queries as data"
  :url "http://github.com/bmeg/ophion"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.7.1"]
                 [com.taoensso/timbre "4.8.0"]
                 [org.immutant/web "2.1.6"]
                 [aleph "0.4.3"]
                 [polaris "0.0.17"]
                 [org.apache.tinkerpop/tinkergraph-gremlin "3.2.3"]
                 [org.janusgraph/janusgraph-core "0.2.0-SNAPSHOT"]
                 [org.janusgraph/janusgraph-cassandra "0.2.0-SNAPSHOT"]
                 [org.janusgraph/janusgraph-es "0.2.0-SNAPSHOT"]
                 [org.janusgraph/janusgraph-berkeleyje "0.2.0-SNAPSHOT"]]
  :repositories [["sonatype snapshots"
                  "https://oss.sonatype.org/content/repositories/snapshots"]
                 ["sonatype releases"
                  "https://oss.sonatype.org/content/repositories/releases"]]
  :jvm-opts ["-Xmx12g" "-Xms12g"]
  :main ophion.aleph)
