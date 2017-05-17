(ns ophion.ingest
  (:require
   [taoensso.timbre :as log]
   [cheshire.core :as json]
   [protograph.kafka :as kafka]
   [protograph.core :as protograph]
   [ophion.db :as db]
   [ophion.query :as query]
   [ophion.search :as search])
  (:import
   [protograph Protograph]))

(defn ingest-vertex
  [graph data]
  (let [vertex (query/add-vertex! graph data)
        id (.id vertex)]
    {:id id
     :data data
     :graph "vertex"}))

(defn ingest-edge
  [graph data]
  (let [edge (query/add-edge! graph data)
        id (.id edge)
        out-id (.getOutVertexId id)
        type-id (.getTypeId id)
        edge-id (.getRelationId id)
        in-id (.getInVertexId id)]
    {:id edge-id
     :out-id out-id
     :label type-id
     :in-id in-id
     :data data
     :graph "edge"}))

(defn ingest-message
  [graph producer prefix continuation message]
  (let [label (kafka/topic->label (.topic message))
        data (json/parse-string (.value message) keyword)
        _ (log/info label data)
        result (condp = label
                 "Vertex" (ingest-vertex graph data)
                 "Edge" (ingest-edge graph data))
        ;; id (.id ingested)
        ;; json (Protograph/writeJSON result)
        ;; result {:id id :data data :graph label}
        ]
    ;; (kafka/send producer (str prefix "." label) json)
    (continuation result)))

(defn ingest-graph
  [config graph continuation]
  (let [host (get-in config [:kafka :host])
        group-id (kafka/uuid)
        prefix (get-in config [:protograph :prefix])
        input-topics [(str prefix ".Vertex") (str prefix ".Edge")]
        output-prefix (get-in config [:protograph :output])
        consumer (kafka/consumer host (kafka/uuid) input-topics)
        producer (kafka/producer host)]
    (kafka/consume
     consumer
     (partial ingest-message graph producer output-prefix continuation))))

(defn -main
  []
  (let [graph (db/connect-graph "config/ophion.clj")
        search (search/connect (assoc search/default-config :index "test"))]
    (ingest-graph
     protograph/default-config
     graph
     (partial search/index-message search))))
