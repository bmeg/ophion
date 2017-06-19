(ns ophion.aggregate
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ophion.mongo :as mongo]))

(defn has
  [state where]
  [{:$match where}])

(defn apply-where
  [query where]
  (if where
    (conj query {:$match where})
    query))

(defn from-edge
  [state {:keys [label where]}]
  (apply-where
   [{:$lookup
     {:from "edge"
      :localField "gid"
      :foreignField "to"
      :as "from"}}
    {:$project
     {:history "$_history"
      :to
      {:$filter
       {:input "$from"
        :as "o"
        :cond
        {:$eq ["$$o.label" label]}}}}}
    {:$unwind "$from"}
    {:$addFields {"from._history" "$history"}}
    {:$replaceRoot {:newRoot "$from"}}]
   where))

(defn to-edge
  [state {:keys [label where]}]
  (apply-where
   [{:$lookup
     {:from "edge"
      :localField "gid"
      :foreignField "from"
      :as "to"}}
    {:$project
     {:history "$_history"
      :to
      {:$filter
       {:input "$to"
        :as "o"
        :cond
        {:$eq ["$$o.label" label]}}}}}
    {:$unwind "$to"}
    {:$addFields {"to._history" "$history" "to.yellol" "1333"}}
    {:$replaceRoot {:newRoot "$to"}}]
   where))

(defn from-vertex
  [state {:keys [where]}]
  (apply-where
   [{:$lookup
     {:from "vertex"
      :localField "from"
      :foreignField "gid"
      :as "from"}}
    {:$unwind "$from"}
    {:$addFields {"from._history" "$_history"}}
    {:$replaceRoot {:newRoot "$from"}}]
   where))

(defn to-vertex
  [state {:keys [where]}]
  (apply-where
   [{:$lookup
     {:from "vertex"
      :localField "to"
      :foreignField "gid"
      :as "to"}}
    {:$unwind "$to"}
    {:$addFields {"to._history" "$_history"}}
    {:$replaceRoot {:newRoot "$to"}}]
   where))

(defn from
  [state where]
  (concat
   (from-edge state where)
   (from-vertex state {})))

(defn to
  [state where]
  (concat
   (to-edge state where)
   (to-vertex state {})))

(defn mark
  [state label]
  (let [path (str "_history." label)]
    [{:$addFields {path "$$ROOT"}}
     {:$project
      {(str path "._history") false
       (str path "._id") false}}]))

(defn select
  [state labels]
  [{:$project
    (reduce
     (fn [project label]
       (assoc project label (str "$_history." label)))
     {} labels)}])

(defn group-count
  [state path]
  [{:$group {:_id (str "$" path) :count {:$sum 1}}}
   {:$project {:key "$_id" :count "$count"}}])

(def steps
  {:where has
   :from-edge from-edge
   :to-edge to-edge
   :from-vertex from-vertex
   :to-vertex to-vertex
   :from from
   :to to
   :mark mark
   :select select
   :group-count group-count})

(defn apply-step
  [steps step state]
  (let [step-key (-> step keys first)
        about (-> step vals last)
        traverse (get steps step-key)]
    (traverse state about)))

(defn translate
  ([query] (translate {} query))
  ([state query]
   (let [warp (map
               (fn [step]
                 (apply-step steps step state))
               query)
         attired (conj (vec warp) [{:$project {:_id false}}])]
     (vec (apply concat attired)))))

(defn evaluate
  [db query]
  (let [aggregate (translate query)]
    (mongo/aggregate db :vertex aggregate)))

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

(defn add-vertex!
  [db vertex]
  (mongo/upsert! db :vertex (assoc (flat vertex) :type "vertex"))
  (mongo/one db :vertex (:gid vertex)))

(defn add-edge!
  [db edge]
  (let [gid (edge-gid edge)]
    (mongo/upsert! db :edge (assoc (flat edge) :gid gid :type "edge"))
    (mongo/one db :edge gid)))

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

