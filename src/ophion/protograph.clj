(ns ophion.protograph
  (:require
   [ophion.kafka :as kafka])
  (:import
   [protograph Protograph ProtographEmitter]))

(defn load
  [path]
  (Protograph/loadProtograph path))

(defn emitter
  [emit-vertex emit-edge]
  (reify ProtographEmitter
    (emitVertex [_ vertex] (emit-vertex vertex))
    (emitEdge [_ edge] (emit-edge edge))))

(defn process
  [protograph emit label data]
  (.processMessage protograph emit label data))

(defn kafka-emitter
  [producer vertex-topic edge-topic]
  (emitter
   (fn [vertex]
     (kafka/send producer vertex-topic vertex))
   (fn [edge]
     (kafka/send producer edge-topic edge))))

(defn transform-message
  [protograph emit label message]
  (let [data (Protograph/readJSON message)]
    (process protograph emit label data)))

(defn transform-kafka
  [protograph consumer producer label]
  (let [emit (kafka-emitter producer "protograph.bmeg.Vertex" "protograph.bmeg.Edge")]
    (kafka/consume
     consumer
     (partial transform-message protograph emit label))))
