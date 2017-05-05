(ns ophion.protograph
  (:import
   [protograph Protograph ProtographEmitter]))

(defn load
  [path]
  (Protograph/loadProtograph path))

(defn emitter
  [emit-vertex emit-edge]
  (reify ProtographEmitter
    (emitVertex [vertex] (emit-vertex vertex))
    (emitEdge [vertex] (emit-vertex vertex))))

(defn process
  [protograph emit label data]
  (.processMessage protograph emit label data))
