(ns ophion.ingest
  (:require
   [taoensso.timbre :as log]
   [cheshire.core :as json]
   [ophion.db :as db]
   [ophion.query :as query]
   [ophion.kafka :as kafka]
   [ophion.protograph :as protograph])
  (:import
   [protograph Protograph]))

(defn ingest-vertex
  [graph vertex]
  (query/add-vertex! graph vertex))

(defn ingest-vertex
  [graph edge]
  (query/add-edge! graph edge))

(defn ingest-message
  [graph producer prefix message]
  (let [label (kafka/topic->label (.topic message))
        data (json/parse-string (.value message) keyword)
        _ (log/info label data)
        ingested (condp = label
                   "Vertex" (query/add-vertex! graph data)
                   "Edge" (query/add-edge! graph data))
        id (.id ingested)
        result {:id id :data data}
        json (Protograph/writeJSON result)]
    (kafka/send producer (str prefix "." label) json)))

(defn ingest-graph
  [config graph]
  (let [host (get-in config [:kafka :host])
        group-id (kafka/uuid)
        prefix (get-in config [:protograph :prefix])
        input-topics [(str prefix ".Vertex") (str prefix ".Edge")]
        output-prefix (get-in config [:protograph :output])
        consumer (kafka/consumer host (kafka/uuid) input-topics)
        producer (kafka/producer host)]
    (kafka/consume
     consumer
     (partial ingest-message graph producer output-prefix))))

(defn -main
  []
  (let [graph (db/connect-graph "config/ophion.clj")]
    (ingest-graph protograph/default-config graph)))
