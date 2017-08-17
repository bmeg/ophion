(ns ophion.mongo-test
  (:require
   [clojure.test :refer :all]
   [taoensso.timbre :as log]
   [ophion.config :as config]
   [ophion.mongo :as mongo]))

(def variant-query
  [{:label "Biosample"}
   {:match
    [[{:in ["hasSample"]}
      {:where {:name "CCLE"}}]
     [{:in ["variantFor"]}
      {:out ["variantIn"]}
      {:where {:symbol "BRAF"}}]]}])

(def variant-mongo
  [{:$match
    {:label "variantFor"
     :to {:$in (map :gid samples)}}}
   {:$project
    {:gid "$from"}}
   {:$skip 0}
   {:$limit 10000}])

(defn biosample-in-cohort
  [])

(defn variant-in-biosample
  [biosample]
  {:$match
   {:label "variantInBiosample"
    :to {:$in (map :gid biosample)}}}
  {:$project
   {:gid "$from"}}
  {:$skip 0}
  {:$limit 10000})

(deftest samples-with-variants
  (testing "finding all samples with a variant in a given gene"
    (let [query []])))

;; ophion gremlin query
[{:has {:key "gid" :value "type:Gene"}}
 {:out ["hasInstance"]}
 {:match
  [[{:as ["a"]}
    {:in ["synonymForGene"]}
    {:has {:key "name" :value "ensembl"}}]
   [{:as ["a"]}
    {:out ["geneInFamily"]}
    {:has {:key "tag" :value "AATP"}}]]}]

;; mongo aggregation query
[{:where {:label "Gene"}}
 {:match
  [["database"
    {:from
     {:label "synonymForGene"
      :where {:from.name "ensembl"}}}]
   ["family"
    {:to
     {:label "geneInFamily"
      :where {:to.tag "AATP"}}}]]}]

(time
 (def oorrr
   (mongo/aggregate
    db :vertex
    [{:$match {:label "Gene"}}
     {:$facet
      {"_root"
       [{:$match {}}],
       "database"
       [{:$addFields {"_history.database" "$$ROOT"}}
        {:$project {"_history.database._history" false, "_history.database._id" false}}
        {:$lookup {:from "edge", :localField "gid", :foreignField "to", :as "from"}}
        {:$unwind "$from"}
        {:$match {:from.label "synonymForGene"}}
        {:$addFields {"from._history" "$_history"}}
        {:$replaceRoot {:newRoot "$from"}}
        {:$lookup {:from "vertex", :localField "from", :foreignField "gid", :as "from"}}
        {:$unwind "$from"}
        {:$match {:from.name "ensembl"}}
        {:$addFields {"from._history" "$_history"}}
        {:$replaceRoot {:newRoot "$from"}}
        {:$project {"database" "$_history.database"}}
        {:$project {:_id false}}
        {:$unwind "$database"}
        {:$group {"_id" "$_id" "database" {:$push "$database.gid"}}}]
       "family"
       [{:$addFields {"_history.family" "$$ROOT"}}
        {:$project {"_history.family._history" false, "_history.family._id" false}}
        {:$lookup {:from "edge", :localField "gid", :foreignField "from", :as "to"}}
        {:$unwind "$to"} {:$match {:to.label "geneInFamily"}}
        {:$addFields {"to._history" "$_history"}}
        {:$replaceRoot {:newRoot "$to"}}
        {:$lookup {:from "vertex", :localField "to", :foreignField "gid", :as "to"}}
        {:$unwind "$to"}
        {:$match {:to.tag "AATP"}}
        {:$addFields {"to._history" "$_history"}}
        {:$replaceRoot {:newRoot "$to"}}
        {:$project {"family" "$_history.family"}}
        {:$project {:_id false}}
        {:$unwind "$family"}
        {:$group {"_id" "$_id" "family" {:$push "$family.gid"}}}]}}
     {:$project
      {:_root true
       :database {:$arrayElemAt ["$database" 0]}
       :family {:$arrayElemAt ["$family" 0]}}}
     {:$project
      {:_root true
       :matches
       {:$setIntersection ["$database.database" "$family.family"]}}}
     {:$unwind "$_root"}
     {:$redact
      {:$cond
       {"if"
        {:$setIsSubset
         [{:$map
           {:input
            {:$literal ["yes"]}
            :as "a"
            :in "$_root.gid"}}
          "$matches"]}
        "then" "$$KEEP"
        "else" "$$PRUNE"}}}
     {:$replaceRoot {:newRoot "$_root"}}
     {:$project {:_id false}}])))
