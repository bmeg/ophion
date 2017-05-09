(ns ophion.db
  (:require
   [ophion.config :as config])
  (:import
   [org.apache.commons.configuration BaseConfiguration]
   [org.janusgraph.core JanusGraphFactory JanusGraph]
   [org.apache.tinkerpop.gremlin.tinkergraph.structure TinkerGraph]))

(defn connect-tinkergraph
  ([] (connect-tinkergraph {}))
  ([config]
   (TinkerGraph/open)))

(defn connect-janus
  [{:keys [host keyspace] :as config}]
  (let [base (BaseConfiguration.)]
    (.setProperty base "storage.backend" "cassandrathrift")
    (.setProperty base "storage.hostname" (or (name host) "localhost"))
    (.setProperty base "storage.cassandra.keyspace" (or (name keyspace) "ophion"))
    (.setProperty base "storage.cassandra.frame-size-mb" "60")
    (JanusGraphFactory/open base)))

(defn connect
  [{:keys [database] :as config}]
  (condp = database
    :janus (connect-janus config)
    :tinkergraph (connect-tinkergraph config)
    (connect-tinkergraph config)))

(defn commit
  [graph]
  (try
    (.. graph tx commit)
    (catch Exception e)))

(defn connect-graph
  [path]
  (let [config (config/read-config path)]
    (connect (:graph config))))
