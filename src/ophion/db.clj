(ns ophion.db
  (:require
   [clojure.string :as string]
   [taoensso.timbre :as log]
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
   (.getPropertyKey manage (name key))
   (.make
    (.dataType
     (.makePropertyKey manage (name key)) type))))

(defn janus-get-edge-label
  [manage edge-label]
  (or
   (.getEdgeLabel manage (name edge-label))
   (.make (.makeEdgeLabel manage (name edge-label)))))

(defn janus-apply-index
  [manage db-index index]
  (let [db (reduce
            (fn [db-index [k v]]
              (let [property (janus-get-property-key manage [k v])]
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
  (string/join
   "-"
   (concat
    [(name edge-label)]
    (sort (map name (keys index)))
    ["vertex-centric-index"])))

(defn janus-edge-index
  [graph edge-label index]
  (try
    (let [index-name (edge-index-name edge-label index)
          manage (.openManagement graph)
          property-keys (into-array (mapv (partial janus-get-property-key manage) index))
          edge (janus-get-edge-label manage edge-label)
          _ (log/info index-name edge property-keys)
          index (.buildEdgeIndex manage edge index-name Direction/BOTH Order/incr property-keys)]
      (.commit manage))
    (catch Exception e
      (.printStackTrace e))))

(defn janus-edge-reindex
  [graph edge-label index]
  (try
    (let [index-name (edge-index-name edge-label index)
          manage (.openManagement graph)
          edge (janus-get-edge-label manage edge-label)
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

(defn rollback
  [graph]
  (try
    (.. graph tx rollback)
    (catch Exception e)))

(defn connect-graph
  [path]
  (let [config (config/read-config path)]
    (connect (:graph config))))

(def indexes
  {:vertexes
   [{:id String}
    {:gid String}
    {:type String}
    {:symbol String}
    {:featureId String}
    {:chromosome String :start String :end String}]
   :edges
   [{:variantInGene {:featureId String}}]})

(defn -main
  [& args]
  (let [graph (connect-graph "config/ophion.clj")
        indexes indexes]
    (doseq [vertex (:vertexes indexes)]
      (janus-property-index graph vertex))
    (doseq [edge (:edges indexes)]
      (janus-edge-index graph (first (keys edge)) (first (vals edge))))))
