(ns ophion.traverse
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ophion.mongo :as mongo]))

(def document-limit 10000)

(defn bufferize-query
  ([db collection query buffer] (bufferize-query db collection query buffer 0))
  ([db collection query buffer page]
   (let [buffered-query (concat query [{:$skip (* buffer page)} {:$limit buffer}])
         results (mongo/aggregate db collection buffered-query {:allow-disk-use true :cursor {:batch-size 10000}})]
     (if (or (empty? results) (< (count results) buffer))
       results
       (lazy-seq
        (concat
         results
         (bufferize-query db collection query buffer (inc page))))))))

(defn where
  [where]
  [{:$match where}])

(defn from-edge
  [db label froms where]
  (bufferize-query
   db :edge
   {:$match
    (merge
     where
     {:label label
      :from {:$in (map :gid froms)}})}
   document-limit))

(defn to-edge
  [db label tos where]
  (bufferize-query
   db :edge
   {:$match
    (merge
     where
     {:label label
      :to {:$in (map :gid tos)}})}
   document-limit))

(defn from-vertex
  [db edges where]
  (bufferize-query
   db :vertex
   {:$match
    (merge
     where
     {:gid {:$in (map :from edges)}})}
   document-limit))

(defn to-vertex
  [db edges where]
  (bufferize-query
   db :vertex
   {:$match
    (merge
     where
     {:gid {:$in (map :from edges)}})}
   document-limit))

(defn from
  ([label]
   (concat
    (from-edge label)
    (from-vertex label))))

(defn to
  ([label]
   (concat
    (to-edge label)
    (to-vertex label))))

(defn values
  [values]
  [{:$project
    (reduce
     (fn [project value]
       (assoc project value (str "$" (name value))))
     {} values)}])

(defn mark
  [label]
  (let [path (str "_history." label)]
    [{:$addFields {path "$$ROOT"}}
     {:$project
      {(str path "._history") false
       (str path "._id") false}}]))

(defn select
  [labels]
  [{:$project
    (reduce
     (fn [project label]
       (assoc project label (str "$_history." label)))
     {} labels)}])

(defn limit
  [n]
  [{:$limit n}])

(defn group-count
  [path]
  [{:$group {:_id (str "$" path) :count {:$sum 1}}}
   {:$project {:key "$_id" :count "$count"}}])

(declare translate)

(defn match
  [queries]
  )

(def order-map
  {:asc 1
   :desc -1})

(defn order
  [fields]
  [{:$sort fields}])

(def steps
  {:where where
   :from-edge from-edge
   :to-edge to-edge
   :from-vertex from-vertex
   :to-vertex to-vertex
   :from from
   :to to
   :values values
   :mark mark
   :select select
   :limit limit
   :order order
   :group-count group-count})

(defn apply-step
  [steps step]
  (let [step-key (-> step keys first)
        about (-> step vals last)
        traverse (get steps step-key)]
    (traverse about)))

(defn translate
  ([query] (translate {} query))
  ([state query]
   (let [warp (map (partial apply-step steps) query)
         attired (conj (vec warp) [{:$project {:_id false}}])]
     (reduce into [] attired))))

(defn evaluate
  ([db query] (evaluate db :vertex query))
  ([db collection query]
   (let [aggregate (translate query)]
     (mongo/aggregate db collection aggregate {:allow-disk-use true :cursor {:batch-size 1000}}))))

(defn flat
  "this will squash properties if they are in the reserved set"
  [element]
  (merge
   (:properties element)
   (dissoc element :properties)))

(defn edge-gid
  [edge]
  (str
   "(" (:from edge) ")"
   "--" (:label edge) "->"
   "(" (:to edge) ")"))

(defn process-vertex
  [vertex]
  (assoc (flat vertex) :type "vertex"))

(defn process-edge
  [edge]
  (let [gid (edge-gid edge)]
    (assoc (flat edge) :gid gid :type "edge")))

(defn add-vertex!
  [db vertex]
  (let [ingest (process-vertex vertex)]
    (mongo/upsert! db :vertex ingest)
    ingest))

(defn add-edge!
  [db edge]
  (let [ingest (process-edge edge)]
    (mongo/upsert! db :edge ingest)
    ingest))

(defn ingest-graph!
  [db {:keys [vertexes edges]}]
  (doseq [vertex vertexes]
    (add-vertex! db vertex))
  (doseq [edge edges]
    (add-edge! db edge)))



