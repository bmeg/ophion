(ns ophion.db
  (:require
   [clojure.string :as string]
   [ophion.config :as config])
  (:import
   [org.apache.commons.configuration BaseConfiguration]
   [org.janusgraph.core JanusGraphFactory JanusGraph]
   [org.apache.tinkerpop.gremlin.structure Vertex Edge]
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

(defn janus-apply-index
  [manage db-index index]
  (let [db (reduce
            (fn [db-index [k v]]
              (let [property (if-let [property (.getPropertyKey manage (name k))]
                               property
                               (.make (.dataType (.makePropertyKey manage (name k)) v)))]
                (.addKey db-index property)))
            db-index index)]
    (.buildCompositeIndex db)))

(defn janus-make-index
  [graph index]
  (try
    (let [index-name (string/join "-" (concat (keys index) ["index"]))
          manage (.openManagement graph)]
      (janus-apply-index manage (.buildIndex manage (str index-name "-vertex") Vertex) index)
      (janus-apply-index manage (.buildIndex manage index-name "-edge" Edge) index)
      (.commit manage))
    (catch Exception e
      (.printStackTrace e))))

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

(defn -main
  [& args]
  (let [graph (connect-graph "config/ophion.clj")]
    (janus-make-index graph {:id String})
    (janus-make-index graph {:gid String})
    (janus-make-index graph {:type String})
    (janus-make-index graph {:label String})
    (janus-make-index graph {:symbol String})
    (janus-make-index graph {:chromosome String :start String :end String})))
