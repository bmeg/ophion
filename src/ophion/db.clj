(ns ophion.db
  (:require
   [clojure.string :as string]
   [ophion.config :as config])
  (:import
   [org.apache.commons.configuration BaseConfiguration]
   [org.janusgraph.core JanusGraphFactory JanusGraph]
   [org.janusgraph.core.schema SchemaAction]
   [org.apache.tinkerpop.gremlin.structure Vertex Edge Direction]
   [org.apache.tinkerpop.gremlin.process.traversal P Order Traversal]
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

(defn janus-get-property-key
  [manage [key type]]
  (or
   (.getPropertyKey manage)
   (.make
    (.dataType
     (.makePropertyKey manage (name key)) type))))

(defn janus-apply-index
  [manage db-index index]
  (let [db (reduce
            (fn [db-index [k v]]
              (let [property (janus-get-property-key manage k v)]
                (.addKey db-index property)))
            db-index index)]
    (.buildCompositeIndex db)))

(defn property-index-name
  [index]
  (string/join "-" (concat (keys index) ["index"])))

(defn janus-property-index
  [graph index]
  (try
    (let [index-name (property-index-name index)
          manage (.openManagement graph)]
      (janus-apply-index manage (.buildIndex manage (str index-name "-vertex") Vertex) index)
      (janus-apply-index manage (.buildIndex manage (str index-name "-edge") Edge) index)
      (.commit manage))
    (catch Exception e
      (.printStackTrace e))))

(defn edge-index-name
  [edge-label index]
  (string/join "-" (concat [edge-label] (keys index) ["vertex-centric-index"])))

(defn janus-edge-index
  [graph edge-label index]
  (try
    (let [index-name (edge-index-name edge-label index)
          manage (.openManagement graph)
          property-keys (into-array (mapv (partial janus-get-property-key index)))
          edge (.getEdgeLabel manage edge-label)
          index (.buildEdgeIndex manage edge index-name Direction/BOTH Order/incr property-keys)]
      (.commit manage))
    (catch Exception e
      (.printStackTrace e))))

(defn janus-edge-reindex
  [graph edge-label index]
  (try
    (let [index-name (edge-index-name edge-label index)
          manage (.openManagement graph)
          edge (.getEdgeLabel manage edge-label)
          index (.getRelationIndex manage edge index-name)]
      (.updateIndex index SchemaAction/REINDEX)
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
    (janus-property-index graph {:id String})
    (janus-property-index graph {:gid String})
    (janus-property-index graph {:type String})
    (janus-property-index graph {:label String})
    (janus-property-index graph {:symbol String})
    (janus-property-index graph {:featureId String})
    (janus-property-index graph {:chromosome String :start String :end String})))
