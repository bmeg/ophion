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

(def steps
  {:where has
   :from-edge from-edge
   :to-edge to-edge
   :from-vertex from-vertex
   :to-vertex to-vertex
   :from from
   :to to
   :mark mark
   :select select})

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
