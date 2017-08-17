(ns ophion.aggregate
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ophion.mongo :as mongo]))

(def document-limit 10000)

(defn where
  [where]
  [{:$match where}])

(defn from-edge
  [{:keys [label where]}]
  [{:$lookup
    {:from "edge"
     :localField "gid"
     :foreignField "to"
     :as "from"}}
   {:$unwind "$from"}
   {:$match
    (merge
     where
     {:from.label label})}
   {:$addFields {"from._history" "$_history"}}
   {:$replaceRoot {:newRoot "$from"}}])

(defn to-edge
  [{:keys [label where]}]
  [{:$lookup
    {:from "edge"
     :localField "gid"
     :foreignField "from"
     :as "to"}}
   {:$unwind "$to"}
   {:$match
    (merge
     where
     {:to.label label})}
   {:$addFields {"to._history" "$_history"}}
   {:$replaceRoot {:newRoot "$to"}}])

(defn from-vertex
  [{:keys [where]}]
  [{:$lookup
    {:from "vertex"
     :localField "from"
     :foreignField "gid"
     :as "from"}}
   {:$unwind "$from"}
   {:$match (or where {})}
   {:$addFields {"from._history" "$_history"}}
   {:$replaceRoot {:newRoot "$from"}}])

(defn to-vertex
  [{:keys [where]}]
  [{:$lookup
    {:from "vertex"
     :localField "to"
     :foreignField "gid"
     :as "to"}}
   {:$unwind "$to"}
   {:$match (or where {})}
   {:$addFields {"to._history" "$_history"}}
   {:$replaceRoot {:newRoot "$to"}}])

(defn from
  ([what]
   (concat
    (from-edge (select-keys what [:label]))
    (from-vertex (select-keys what [:where])))))

(defn to
  ([what]
   (concat
    (to-edge (select-keys what [:label]))
    (to-vertex (select-keys what [:where])))))

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

(def order-map
  {:asc 1
   :desc -1})

(defn qount
  [label]
  [{:$count label}])

(defn offset
  [n]
  [{:$skip n}])

(defn order
  [fields]
  [{:$sort fields}])

(defn root
  [in]
  [{:$replaceRoot {:newRoot (str "$" in)}}])

(defn group-count
  [path]
  [{:$group {:_id (str "$" path) :count {:$sum 1}}}
   {:$project {:key "$_id" :count "$count"}}])

(declare translate)

(defn match
  [sub]
  (let [queries
        (reduce
         (fn [queries s]
           (let [label (first s)
                 query (concat
                        [{:mark label}]
                        (rest s)
                        [{:select [label]}
                         {:root label}])]
             (assoc queries label (translate query))))
         {} sub)]
    [{:$facet queries}]))

     ;; {:$setIntersection (mapv (comp (partial str "$") first) queries)}

(def steps
  {:from-edge from-edge
   :to-edge to-edge
   :from-vertex from-vertex
   :to-vertex to-vertex
   :from from
   :to to
   :root root
   :where where
   :match match
   :values values
   :mark mark
   :select select
   :limit limit
   :order order
   :offset offset
   :count qount
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
     (log/info aggregate)
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



(def gods-graph
  {:vertexes
   [{:label "location" :gid "sky" :properties {:name "sky"}}
    {:label "location" :gid "sea" :properties {:name "sea"}}
    {:label "location" :gid "tartarus" :properties {:name "tartarus"}}
    {:label "titan" :gid "saturn" :properties {:name "saturn" :age 5000}}
    {:label "titan" :gid "saturn" :properties {:name "saturn" :age 10000}}
    {:label "god" :gid "jupiter" :properties {:name "jupiter" :age 5000}}
    {:label "god" :gid "neptune" :properties {:name "neptune" :age 4500}}
    {:label "god" :gid "pluto" :properties {:name "pluto"}}
    {:label "god" :gid "pluto" :properties {:age 4000}}
    {:label "demigod" :gid "hercules" :properties {:name "hercules" :age 30}}
    {:label "human" :gid "alcmene" :properties {:name "alcmene" :age 45}}
    {:label "monster" :gid "nemean" :properties {:name "nemean"}}
    {:label "monster" :gid "hydra" :properties {:name "hydra"}}
    {:label "monster" :gid "cerberus" :properties {:name "cerberus"}}]
   :edges
   [{:from-label "god" :from "jupiter" :label "father" :to-label "god" :to "saturn"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "jupiter" :label "lives" :to-label "location" :to "sky" :properties {:reason "likes wind" :much 0.3}}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "neptune" :label "lives" :to-label "location" :to "sea" :properties {:reason "likes waves" :much 0.4}}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "pluto" :label "lives" :to-label "location" :to "tartarus" :properties {:reason "likes death" :much 0.5}}
    {:from-label "demigod" :from "hercules" :label "father" :to-label "god" :to "jupiter"}
    {:from-label "demigod" :from "hercules" :label "mother" :to-label "human" :to "alcmene"}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "nemean" :properties {:trial 1}}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "hydra" :properties {:trial 2}}
    {:from-label "monster" :from "hercules" :label "battled" :to-label "monster" :to "cerberus" :properties {:trial 12}}
    {:from-label "monster" :from "cerberus" :label "lives" :to-label "location" :to "tartarus"}]})

;; [{:$match {:label "Biosample"}}
;;  {:$lookup {:from "edge", :localField "gid", :foreignField "to", :as "to"}}
;;  {:$unwind "$to"}
;;  {:$match {:to.label "hasSample"}}
;;  {:$addFields {"to._history" "$history"}}
;;  {:$replaceRoot {:newRoot "$to"}}
;;  {:$lookup {:from "vertex", :localField "from", :foreignField "gid", :as "to"}}
;;  {:$unwind "$to"}
;;  {:$match {}}
;;  {:$addFields {"to._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$to"}}
;;  {:$group {:_id "$name", :count {:$sum 1}}}
;;  {:$project {:key "$_id", :count "$count"}}]
